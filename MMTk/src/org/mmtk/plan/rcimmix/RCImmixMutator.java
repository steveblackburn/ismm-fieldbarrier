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
package org.mmtk.plan.rcimmix;


import static org.mmtk.plan.Plan.*;
import static org.mmtk.policy.rcimmix.RCImmixConstants.BYTES_IN_LINE;
import static org.mmtk.utility.Constants.*;

import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.policy.LargeObjectLocal;
import org.mmtk.policy.Space;
import org.mmtk.policy.rcimmix.RCImmixMutatorLocal;
import org.mmtk.policy.rcimmix.RCImmixObjectHeader;
import org.mmtk.utility.FieldMarks;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.deque.AddressPairDeque;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.*;

/**
 * This class implements the mutator context for RCImmix collector.
 */
@Uninterruptible
public class RCImmixMutator extends StopTheWorldMutator {

  /************************************************************************
   * Instance fields
   */
  protected final RCImmixMutatorLocal rc;
  private final LargeObjectLocal rclos;
  private final ObjectReferenceDeque modObjectBuffer;
  private final AddressPairDeque modFieldBuffer;
  private final RCImmixDecBuffer decBuffer;

  /************************************************************å************
   *
   * Initialization
   */

  /**
   * Constructor. One instance is created per physical processor.
   */
  public RCImmixMutator() {
    rc = new RCImmixMutatorLocal(RCImmix.rcSpace, false);
    rclos = new LargeObjectLocal(RCImmix.rcloSpace);
    modObjectBuffer = new ObjectReferenceDeque("mod", global().modObjectPool);
    modFieldBuffer = new AddressPairDeque("mod field", global().modFieldPool);
    decBuffer = new RCImmixDecBuffer(global().decPool);
  }

  /****************************************************************************
   *
   * Mutator-time allocation
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public Address alloc(int bytes, int align, int offset, int allocator, int site) {
    switch (allocator) {
      case RCImmix.ALLOC_DEFAULT:
        return rc.alloc(bytes, align, offset);
      case RCImmix.ALLOC_LOS:
      case RCImmix.ALLOC_PRIMITIVE_LOS:
      case RCImmix.ALLOC_LARGE_CODE:
        return rclos.alloc(bytes, align, offset);
      case RCImmix.ALLOC_NON_MOVING:
      case RCImmix.ALLOC_CODE:
      case RCImmix.ALLOC_IMMORTAL:
        return super.alloc(bytes, align, offset, allocator, site);
      default:
        VM.assertions.fail("Allocator not understood by RC");
        return Address.zero();
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference ref, ObjectReference typeRef, int bytes, int allocator) {
    switch (allocator) {
      case RCImmix.ALLOC_DEFAULT:
        if (bytes > BYTES_IN_LINE) RCImmixObjectHeader.initializeHeader(ref);
        break;
      case RCImmix.ALLOC_LOS:
      case RCImmix.ALLOC_PRIMITIVE_LOS:
      case RCImmix.ALLOC_LARGE_CODE:
        decBuffer.push(ref);
        RCImmix.rcloSpace.initializeHeader(ref, true);
        RCImmixObjectHeader.initializeHeaderOther(ref, true);
        return;
      case RCImmix.ALLOC_NON_MOVING:
      case RCImmix.ALLOC_CODE:
      case RCImmix.ALLOC_IMMORTAL:
        decBuffer.push(ref);
        RCImmixObjectHeader.initializeHeaderOther(ref, true);
        return;
      default:
        VM.assertions.fail("Allocator not understood by RC");
        return;
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == RCImmix.rcSpace) return rc;
    if (space == RCImmix.rcloSpace) return rclos;
    return super.getAllocatorFromSpace(space);
  }

  /****************************************************************************
   *
   * Collection
   */

  /**
   * {@inheritDoc}
   */
  @Override
  public void collectionPhase(short phaseId, boolean primary) {
    if (phaseId == RCImmix.PREPARE) {
      rc.prepare();
      return;
    }

    if (phaseId == RCImmix.PROCESS_MODBUFFER) {
      modObjectBuffer.flushLocal();
      modFieldBuffer.flushLocal();
      return;
    }

    if (phaseId == RCImmix.PROCESS_DECBUFFER) {
      decBuffer.flushLocal();
      return;
    }

    if (phaseId == RCImmix.RELEASE) {
      rc.release();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(modObjectBuffer.isEmpty());
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(modFieldBuffer.isEmpty());
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(decBuffer.isEmpty());
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public final void flushRememberedSets() {
    decBuffer.flushLocal();
    modObjectBuffer.flushLocal();
    modFieldBuffer.flushLocal();
    assertRemsetsFlushed();
  }

  @Override
  public final void assertRemsetsFlushed() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(decBuffer.isFlushed());
      VM.assertions._assert(modObjectBuffer.isFlushed());
      VM.assertions._assert(modFieldBuffer.isFlushed());
    }
  }

  @Override
  public void flush() {
    super.flush();
  }

  /****************************************************************************
   *
   * Write barriers.
   */

  /**
   * {@inheritDoc}
   */
  @Override
  @Inline
  public void objectReferenceWrite(ObjectReference src, Address slot,
                                   ObjectReference tgt, Word metaDataA,
                                   Word metaDataB, Word metaDataC, int mode) {
    if (FIELD_BARRIER_STATS) { if (mode != ARRAY_ELEMENT) Plan.pffast.inc(); else Plan.aafast.inc(); }
    if (USE_FIELD_BARRIER_FOR_AASTORE && mode == ARRAY_ELEMENT)
      FieldMarks.refArrayCoalescingBarrier(src, slot, metaDataC, modFieldBuffer, decBuffer);
    else if (USE_FIELD_BARRIER_FOR_PUTFIELD && mode != ARRAY_ELEMENT)
      FieldMarks.scalarFieldCoalescingBarrier(src, slot, metaDataC, modFieldBuffer, decBuffer);
    else if (RCImmixObjectHeader.logRequired(src)) {
      if (FIELD_BARRIER_STATS) { if (mode != ARRAY_ELEMENT) Plan.pfslow.inc(); else Plan.aaslow.inc(); }
      coalescingWriteBarrierSlow(src);
    }
    VM.barriers.objectReferenceWrite(src, tgt, metaDataA, metaDataB, mode);
  }

  @Override
  @Inline
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot,
                                                  ObjectReference old, ObjectReference tgt, Word metaDataA,
                                                  Word metaDataB, Word metaDataC, int mode) {
    if (FIELD_BARRIER_STATS) { if (mode != ARRAY_ELEMENT) Plan.pffast.inc(); else Plan.aafast.inc(); }
    if (USE_FIELD_BARRIER_FOR_AASTORE && mode == ARRAY_ELEMENT)
      FieldMarks.refArrayCoalescingBarrier(src, slot, metaDataC, modFieldBuffer, decBuffer);
    else if (USE_FIELD_BARRIER_FOR_PUTFIELD && mode != ARRAY_ELEMENT)
      FieldMarks.scalarFieldCoalescingBarrier(src, slot, metaDataC, modFieldBuffer, decBuffer);
    else if (RCImmixObjectHeader.logRequired(src)) {
      if (FIELD_BARRIER_STATS) { if (mode != ARRAY_ELEMENT) Plan.pfslow.inc(); else Plan.aaslow.inc(); }
      coalescingWriteBarrierSlow(src);
    }
    return VM.barriers.objectReferenceTryCompareAndSwap(src,old,tgt,metaDataA,metaDataB,mode);
  }

  /**
   * A number of references are about to be copied from object
   * <code>src</code> to object <code>dst</code> (as in an array
   * copy).  Thus, <code>dst</code> is the mutated object.  Take
   * appropriate write barrier actions.<p>
   *
   * @param src The source of the values to be copied
   * @param srcOffset The offset of the first source address, in
   * bytes, relative to <code>src</code> (in principle, this could be
   * negative).
   * @param dst The mutated object, i.e. the destination of the copy.
   * @param dstOffset The offset of the first destination address, in
   * bytes relative to <code>tgt</code> (in principle, this could be
   * negative).
   * @param bytes The size of the region being copied, in bytes.
   * @return True if the update was performed by the barrier, false if
   * left to the caller (always false in this case).
   */
  @Override
  @Inline
  public boolean objectReferenceBulkCopy(ObjectReference src, Offset srcOffset,
                              ObjectReference dst, Offset dstOffset, int bytes) {
    if (USE_FIELD_BARRIER_FOR_AASTORE) { // FIXME: assumption that this only applies to arrays
      coalescingFieldWriteBarrierBulkCopySlow(dst, dstOffset, bytes);
    } else if (RCImmixObjectHeader.logRequired(dst)) {
      coalescingWriteBarrierSlow(dst);
    }
    return false;
  }

  /**
   * Slow path of the coalescing write barrier.
   *
   * <p> Attempt to log the source object. If successful in racing for
   * the log bit, push an entry into the modified buffer and add a
   * decrement buffer entry for each referent object (in the RC space)
   * before setting the header bit to indicate that it has finished
   * logging (allowing others in the race to continue).
   *
   * @param srcObj The object being mutated
   */
  @NoInline
  private void coalescingWriteBarrierSlow(ObjectReference srcObj) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!USE_FIELD_BARRIER_FOR_PUTFIELD || !USE_FIELD_BARRIER_FOR_AASTORE);
    if (RCImmixObjectHeader.attemptToLogObject(srcObj)) {
      if (FIELD_BARRIER_STATS) Plan.pfwordsLogged.inc(); // mod buffer
      modObjectBuffer.push(srcObj);
      decBuffer.processChildren(srcObj);
      RCImmixObjectHeader.makeLogged(srcObj);
    }
  }

  private void coalescingFieldWriteBarrierBulkCopySlow(ObjectReference dst, Offset dstOffset, int bytes) {
    // log each of the to-be-overwritten fields
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(USE_FIELD_BARRIER_FOR_AASTORE);
    Address cursor = dst.toAddress().plus(dstOffset);
    Address end = cursor.plus(bytes);
    int index = dstOffset.toInt() >> LOG_BYTES_IN_ADDRESS;
    while (cursor.LT(end)) {
      if (FIELD_BARRIER_STATS) bulkWordsLogged.inc();
      FieldMarks.refArrayCoalescingBarrier(dst, cursor, Word.fromIntSignExtend(index), modFieldBuffer, decBuffer);
      cursor = cursor.plus(BYTES_IN_ADDRESS);
      index++;
    }
  }

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>RC</code> instance. */
  @Inline
  private static RCImmix global() {
    return (RCImmix) VM.activePlan.global();
  }
}
