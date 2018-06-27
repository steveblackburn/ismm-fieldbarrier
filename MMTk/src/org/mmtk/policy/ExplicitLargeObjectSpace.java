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
package org.mmtk.policy;

import static org.mmtk.plan.refcount.RCBase.USE_FIELD_BARRIER;
import static org.mmtk.utility.Constants.BYTES_IN_ADDRESS;
import static org.mmtk.utility.Constants.LOG_BYTES_IN_PAGE;


import org.mmtk.plan.Plan;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.utility.Log;
import org.mmtk.utility.heap.FreeListPageResource;
import org.mmtk.utility.heap.VMRequest;
import org.mmtk.utility.DoublyLinkedList;

import org.mmtk.vm.VM;

import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.*;

/**
 * Each instance of this class corresponds to one explicitly managed
 * large object space.
 */
@Uninterruptible
public final class ExplicitLargeObjectSpace extends BaseLargeObjectSpace {

  private static final int LOG_CELL_GRANULARITY = LOG_BYTES_IN_PAGE;
  private static final Word CELL_MASK = Word.fromIntSignExtend((1<<LOG_CELL_GRANULARITY)-1).not();
  private static final Offset OBJECT_PTR_OFFSET = DoublyLinkedList.HEADER_SIZE;
  private static final int CELL_HEADER_SIZE = BYTES_IN_ADDRESS;


  /****************************************************************************
   *
   * Instance variables
   */

  /**
   *
   */
  private final DoublyLinkedList cells;

  /****************************************************************************
   *
   * Initialization
   */

  /**
   * The caller specifies the region of virtual memory to be used for
   * this space.  If this region conflicts with an existing space,
   * then the constructor will fail.
   *
   * @param name The name of this space (used when printing error messages etc)
   * @param vmRequest An object describing the virtual memory requested.
   */
  public ExplicitLargeObjectSpace(String name, VMRequest vmRequest) {
    this(name, true, vmRequest);
  }

  /**
   * The caller specifies the region of virtual memory to be used for
   * this space.  If this region conflicts with an existing space,
   * then the constructor will fail.
   *
   * @param name The name of this space (used when printing error messages etc)
   * @param zeroed if {@code true}, allocations return zeroed memory.
   * @param vmRequest An object describing the virtual memory requested.
   */
  public ExplicitLargeObjectSpace(String name, boolean zeroed, VMRequest vmRequest) {
    super(name, zeroed, vmRequest);
    cells = new DoublyLinkedList(LOG_CELL_GRANULARITY, true);
  }

  /****************************************************************************
   *
   * Collection
   */

  /**
   * Prepare for a new collection increment.
   */
  public void prepare() {
  }

  /**
   * A new collection increment has completed.
   */
  public void release() {
  }

  /**
   * Release a group of pages that were allocated together.
   *
   * @param first The first page in the group of pages that were
   * allocated together.
   */
  @Override
  @Inline
  public void release(Address first) {
    ((FreeListPageResource) pr).releasePages(first);
  }

  private static final Address getCell(ObjectReference object) {
    if (VM.VERIFY_ASSERTIONS && USE_FIELD_BARRIER) VM.assertions._assert(VM.objectModel.isArray(object));
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Plan.FIELD_BARRIER_USE_BYTE);
    if (VM.objectModel.isPrimitiveArray(object)) {
      Log.write(" P: "); Log.writeln(object);
      return getCell(object.toAddress());
    } else {
      Log.write(" R: "); Log.writeln(object);
      return getCell(object.toAddress().minus(VM.objectModel.getArrayLength(object)));  // FIXME NEED TO ADJUST FOR BIT MARKS
    }
  }

  private static final Address getCell(Address address) {
    return address.toWord().and(CELL_MASK).toAddress();
  }

  private static final ObjectReference getObject(Address cell) {
    return cell.plus(OBJECT_PTR_OFFSET).loadObjectReference();
  }

  private static final void setObject(Address cell, ObjectReference obj) {
    cell.plus(OBJECT_PTR_OFFSET).store(obj);
  }
  /**
   * Perform any required initialization of the GC portion of the header.
   *
   * @param object the object ref to the storage to be initialized
   * @param alloc is this initialization occuring due to (initial) allocation
   * ({@code true}) or due to copying (<code>false</code>)?
   */
  @Inline
  public void initializeHeader(ObjectReference object, boolean alloc) {
    Log.write("IH: ", object);  Log.write(" l: ", VM.objectModel.getArrayLength(object));
    Log.writeln(" c: ", getCell(object));
    cells.checkHead("IH ");
    cells.add(DoublyLinkedList.midPayloadToNode(getCell(object)));
    setObject(getCell(object), object);
    cells.checkHead("IH~ ");
  }

  /****************************************************************************
   *
   * Object processing and tracing
   */

  /**
   * Trace a reference to an object under a mark sweep collection
   * policy.  If the object header is not already marked, mark the
   * object in either the bitmap or by moving it off the treadmill,
   * and enqueue the object for subsequent processing. The object is
   * marked as (an atomic) side-effect of checking whether already
   * marked.
   *
   * @param trace The trace being conducted.
   * @param object The object to be traced.
   * @return The object (there is no object forwarding in this
   * collector, so we always return the same object: this could be a
   * void method but for compliance to a more general interface).
   */
  @Override
  @Inline
  public ObjectReference traceObject(TransitiveClosure trace, ObjectReference object) {
    return object;
  }

  /**
   * @param object The object in question
   * @return {@code true} if this object is known to be live (i.e. it is marked)
   */
  @Override
  @Inline
  public boolean isLive(ObjectReference object) {
    return true;
  }

  @Override
  @Inline
  protected int superPageHeaderSize() {
    return DoublyLinkedList.headerSize();
  }

  @Override
  @Inline
  protected int cellHeaderSize() {
    return CELL_HEADER_SIZE;
  }

  /**
   * Sweep through all the objects in this space.
   *
   * @param sweeper The sweeper callback to use.
   */
  @Inline
  public void sweep(Sweeper sweeper) {
    Address cell = cells.getHead();
    while (!cell.isZero()) {
      Address next = cells.getNext(cell);
      ObjectReference obj = getObject(cell);
      if (sweeper.sweepLargeObject(obj)) {
        free(obj);
      }
      cell = next;
    }
  }

  /**
   * Free an object
   *
   * @param object The object to be freed.
   */
  @Inline
  public void free(ObjectReference object) {
    Log.writeln("F: ", object);
    Address cell = getCell(object);
    cells.remove(cell);
    release(cell);
  }

  /**
   * A callback used to perform sweeping of the large object space.
   */
  @Uninterruptible
  public abstract static class Sweeper {
    public abstract boolean sweepLargeObject(ObjectReference object);
  }
}
