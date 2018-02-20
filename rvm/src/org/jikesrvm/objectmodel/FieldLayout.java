/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */
package org.jikesrvm.objectmodel;

import static org.jikesrvm.mm.mminterface.MemoryManagerConstants.USE_FIELD_BARRIER_FOR_PUTFIELD;
import static org.jikesrvm.mm.mminterface.MemoryManagerConstants.USE_PREFIX_FIELD_MARKS_FOR_SCALARS;
import static org.jikesrvm.objectmodel.JavaHeader.OBJECT_REF_OFFSET;
import static org.jikesrvm.objectmodel.JavaHeader.SCALAR_HEADER_SIZE;
import static org.jikesrvm.objectmodel.JavaHeaderConstants.LOG_MIN_ALIGNMENT;
import static org.jikesrvm.runtime.JavaSizeConstants.BYTES_IN_LONG;
import static org.jikesrvm.runtime.UnboxedSizeConstants.BYTES_IN_ADDRESS;
import static org.mmtk.utility.Constants.MIN_ALIGNMENT;

import org.jikesrvm.VM;
import org.jikesrvm.classloader.RVMClass;
import org.jikesrvm.classloader.RVMField;
import org.vmmagic.unboxed.Offset;

/**
 * This abstract class defines the interface for schemes that layout fields
 * in an object.  Not header fields, (scalar) object fields.
 * <p>
 * The field layout object encapsulates layout state.
 */
public abstract class FieldLayout {

  /**
   * Enable debugging
   */
  protected static final boolean DEBUG = false;

  /** Whether to lay out 8byte values first in order to avoid some holes */
  private final boolean largeFieldsFirst;

  /** Lay out reference fields in a block */
  private final boolean clusterReferenceFields;

  public FieldLayout(boolean largeFieldsFirst, boolean clusterReferenceFields) {
    this.largeFieldsFirst = largeFieldsFirst;
    this.clusterReferenceFields = clusterReferenceFields;
  }

  /**
   * @param x the integer
   * @return log base 2 of an integer
   */
  protected static int log2(int x) {
    int logSize = 0;
    while ((1 << logSize) < x) {
      logSize += 1;
    }
    return logSize;
  }

  /*
  * Abstract methods that determine the behaviour of a particular layout scheme
  */

  /**
   * Return the appropriate layout context object for the given class.
   *
   * @param klass The class
   * @return The layout context
   */
  protected abstract FieldLayoutContext getLayoutContext(RVMClass klass);

  /**
   * This is where a class gets laid out.  Differences in layout strategy
   * are largely encapsulated in the layoutContext object.
   *
   * @param klass The class to lay out.
   */
  public void layoutInstanceFields(RVMClass klass) {
    /*
     * Determine available field slots from parent classes, and allocate
     * a new context object for this class and its children.
     */
    FieldLayoutContext fieldLayout = getLayoutContext(klass);

    // Preferred alignment of object - modified to reflect added fields
    // New fields to be allocated for this object
    RVMField[] fields = klass.getDeclaredFields();

    if (DEBUG) {
      VM.sysWriteln("Laying out: ", klass.toString());
    }

    /*
    * Layout reference fields first pre-pass - This can help some
    * GC schemes.
    */
    if (clusterReferenceFields) {
      // For every field
      for (RVMField field : fields) {
        if (!field.isStatic() && !field.hasOffset()) {
          if (field.isReferenceType()) {
            layoutField(fieldLayout, klass, field, BYTES_IN_ADDRESS);
          }
        }
      }
    }

    /*
    * Layout 8byte values first pre-pass - do this to avoid unnecessary
    * holes for object layouts such as an int followed by a long
    */
    if (largeFieldsFirst) {
      // For every field
      for (RVMField field : fields) {
        // Should we allocate space in the object now?
        if (!field.isStatic() && !field.hasOffset()) {
          if (field.getSize() == BYTES_IN_LONG) {
            layoutField(fieldLayout, klass, field, BYTES_IN_LONG);
          }
        }
      }
    }

    for (RVMField field : fields) {                               // For every field
      int fieldSize = field.getSize();                            // size of field
      if (!field.isStatic() && !field.hasOffset()) {              // Allocate space in the object?
        layoutField(fieldLayout, klass, field, fieldSize);
      }
    }
    // JavaHeader requires objects to be int sized/aligned
    if (VM.VerifyAssertions) VM._assert((fieldLayout.getObjectSize() & 0x3) == 0);

    /* Update class to reflect changes */

    updateClass(klass, fieldLayout);
  }

  /**
   * Updates the RVMClass with context info.
   *
   * @param klass the class to update
   * @param fieldLayout the layout for the class
   */
  protected void updateClass(RVMClass klass, FieldLayoutContext fieldLayout) {
    /*
     * Save the new field layout.
     */
    klass.setFieldLayoutContext(fieldLayout);

    int size = fieldLayout.getObjectSize();
    if (USE_FIELD_BARRIER_FOR_PUTFIELD) {
      int numReferences = (size + 3) >> 2; // conservative estimate
      int fieldMarkBytes = ObjectModel.fieldMarkBytes(numReferences);
      klass.setAlignedFieldMarkBytes(fieldMarkBytes);
      if (!USE_PREFIX_FIELD_MARKS_FOR_SCALARS) {
        klass.setInstanceMarkStateOffsetInternal(size - (OBJECT_REF_OFFSET - SCALAR_HEADER_SIZE));
        size += fieldMarkBytes;
      }
    }

    klass.setInstanceSizeInternal(ObjectModel.computeScalarHeaderSize(klass) + size);
    klass.setAlignment(fieldLayout.getAlignment());
  }

  /**
   * Update a field to set its offset within the object.
   *
   * @param klass the class that the field belongs to
   * @param field the field
   * @param offset the new offset for the field
   */
  protected void setOffset(RVMClass klass, RVMField field, int offset) {

    Offset fieldOffset;
    if (offset >= 0) {
      fieldOffset =
          Offset.fromIntSignExtend(JavaHeader.objectStartOffset(klass) +
                                   ObjectModel.computeScalarHeaderSize(klass) +
                                   offset);
    } else {
      /* Negative offsets go before the header */
      fieldOffset = Offset.fromIntSignExtend(JavaHeader.objectStartOffset(klass) + offset);
    }
    field.setOffset(fieldOffset);
    if (DEBUG) {
      VM.sysWrite("  field: ", field.toString());
      VM.sysWriteln(" offset ", fieldOffset.toInt());
    }
  }

  /**
   * Lay out a given field.
   *
   * @param layout State for the layout process
   * @param klass The class whose fields we're laying out.
   * @param field The field we are laying out.
   * @param fieldSize The size of the field in bytes
   */
  protected void layoutField(FieldLayoutContext layout, RVMClass klass, RVMField field, int fieldSize) {
    boolean isRef = field.isReferenceType();
    setOffset(klass, field, layout.nextOffset(fieldSize, isRef));
  }
}
