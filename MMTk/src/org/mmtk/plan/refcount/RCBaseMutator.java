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
package org.mmtk.plan.refcount;

import org.mmtk.plan.StopTheWorldMutator;
import org.mmtk.plan.refcount.backuptrace.BTSweepImmortalScanner;
import org.mmtk.policy.ExplicitFreeListLocal;
import org.mmtk.policy.ExplicitFreeListSpace;
import org.mmtk.policy.LargeObjectLocal;
import org.mmtk.policy.Space;
import org.mmtk.utility.alloc.Allocator;
import org.mmtk.utility.deque.AddressPairDeque;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.VM;

import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.*;

import static org.mmtk.plan.refcount.RCBase.BUILD_FOR_GENRC;
import static org.mmtk.plan.refcount.RCBase.USE_FIELD_BARRIER;
import static org.mmtk.utility.Constants.BYTES_IN_ADDRESS;
import static org.mmtk.utility.Constants.LOG_BYTES_IN_ADDRESS;

/**
 * This class implements the mutator context for a reference counting collector.
 * See Shahriyar et al for details of and rationale for the optimizations used
 * here (http://dx.doi.org/10.1145/2258996.2259008).  See Chapter 4 of
 * Daniel Frampton's PhD thesis for details of and rationale for the cycle
 * collection strategy used by this collector.
 */
@Uninterruptible
public class RCBaseMutator extends StopTheWorldMutator {

  /************************************************************************
   * Instance fields
   */

  /**
   *
   */
  private final ExplicitFreeListLocal rc;
  private final LargeObjectLocal rclos;
  private final ObjectReferenceDeque modObjectBuffer;
  private final AddressPairDeque modFieldBuffer;
  private final RCDecBuffer decBuffer;
  private final BTSweepImmortalScanner btSweepImmortal;

  /************************************************************************
   *
   * Initialization
   */

  /**
   * Constructor. One instance is created per physical processor.
   */
  public RCBaseMutator() {
    rc = new ExplicitFreeListLocal(RCBase.rcSpace);
    rclos = new LargeObjectLocal(RCBase.rcloSpace);
    modObjectBuffer = new ObjectReferenceDeque("mod obj", global().modObjectPool);
    modFieldBuffer = new AddressPairDeque("mod field", global().modFieldPool);
    decBuffer = new RCDecBuffer(global().decPool);
    btSweepImmortal = new BTSweepImmortalScanner();
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
      case RCBase.ALLOC_DEFAULT:
      case RCBase.ALLOC_NON_MOVING:
      case RCBase.ALLOC_CODE:
        return rc.alloc(bytes, align, offset);
      case RCBase.ALLOC_LOS:
      case RCBase.ALLOC_PRIMITIVE_LOS:
      case RCBase.ALLOC_LARGE_CODE:
        return rclos.alloc(bytes, align, offset);
      case RCBase.ALLOC_IMMORTAL:
        return super.alloc(bytes, align, offset, allocator, site);
      default:
        VM.assertions.fail("Allocator not understood by RC");
        return Address.zero();
    }
  }

  @Override
  @Inline
  public void postAlloc(ObjectReference ref, ObjectReference typeRef, int bytes, int allocator) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!(BUILD_FOR_GENRC && USE_FIELD_BARRIER)); // FIXME: how does the field barrier affect the following cases?
    switch (allocator) {
    case RCBase.ALLOC_DEFAULT:
    case RCBase.ALLOC_NON_MOVING:
      if (BUILD_FOR_GENRC) modObjectBuffer.push(ref);
    case RCBase.ALLOC_CODE:
      if (BUILD_FOR_GENRC) {
        decBuffer.push(ref);
        RCHeader.initializeHeader(ref, true);
        ExplicitFreeListSpace.unsyncSetLiveBit(ref);
      }
      break;
    case RCBase.ALLOC_LOS:
      if (BUILD_FOR_GENRC) modObjectBuffer.push(ref);
    case RCBase.ALLOC_PRIMITIVE_LOS:
    case RCBase.ALLOC_LARGE_CODE:
      decBuffer.push(ref);
      if (BUILD_FOR_GENRC) RCHeader.initializeHeader(ref, true);
      RCBase.rcloSpace.initializeHeader(ref, true);
      return;
    case RCBase.ALLOC_IMMORTAL:
      if (BUILD_FOR_GENRC) modObjectBuffer.push(ref);
      decBuffer.push(ref);
      if (BUILD_FOR_GENRC) RCHeader.initializeHeader(ref, true);
      return;
    default:
      VM.assertions.fail("Allocator not understood by RC");
      return;
    }
  }

  @Override
  public Allocator getAllocatorFromSpace(Space space) {
    if (space == RCBase.rcSpace) return rc;
    if (space == RCBase.rcloSpace) return rclos;
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
    if (phaseId == RCBase.PREPARE) {
      rc.prepare();
      return;
    }

    if (phaseId == RCBase.PROCESS_MODBUFFER) {
      modObjectBuffer.flushLocal();
      modFieldBuffer.flushLocal();
      return;
    }

    if (phaseId == RCBase.PROCESS_DECBUFFER) {
      decBuffer.flushLocal();
      return;
    }

    if (phaseId == RCBase.RELEASE) {
      if (RCBase.CC_BACKUP_TRACE && RCBase.performCycleCollection) {
        immortal.linearScan(btSweepImmortal);
      }
      rc.release();
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(modObjectBuffer.isEmpty());
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(modFieldBuffer.isEmpty());
      if (!BUILD_FOR_GENRC) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(decBuffer.isEmpty());
      }
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
    rc.flush();
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
                           Word metaDataB, int mode, int markOffset) {
    if (USE_FIELD_BARRIER) {
      if (VM.objectModel.isFieldUnlogged(src, markOffset))
        coalescingFieldWriteBarrierSlow(src, slot, markOffset);
    } else if (RCHeader.logRequired(src)) {
      coalescingObjectWriteBarrierSlow(src);
    }
    VM.barriers.objectReferenceWrite(src,tgt,metaDataA, metaDataB, mode);
  }

  @Override
  @Inline
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot,
                                               ObjectReference old, ObjectReference tgt, Word metaDataA,
                                               Word metaDataB, int mode, int markOffset) {
    if (USE_FIELD_BARRIER) {
      if (VM.objectModel.isFieldUnlogged(src, markOffset)) {
        coalescingFieldWriteBarrierSlow(src, slot, markOffset);
      }
    } else if (RCHeader.logRequired(src)) {
      coalescingObjectWriteBarrierSlow(src);
    }
    return VM.barriers.objectReferenceTryCompareAndSwap(src,old,tgt,metaDataA,metaDataB,mode);
  }

  /**
   * {@inheritDoc}
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
 //   if (USE_FIELD_BARRIER) { // FIXME: assumption that this only applies to arrays
 //     coalescingFieldWriteBarrierSlow(dst, dstOffset, bytes);
  //  }

    if (RCHeader.logRequired(dst)) {
      coalescingObjectWriteBarrierSlow(dst);
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
  private void coalescingObjectWriteBarrierSlow(ObjectReference srcObj) {
   // if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!USE_FIELD_BARRIER);
    if (RCHeader.attemptToLogObject(srcObj)) {
      modObjectBuffer.push(srcObj);
      decBuffer.processChildren(srcObj);
      RCHeader.makeLogged(srcObj);
    }
  }

  @NoInline
  private void coalescingFieldWriteBarrierSlow(ObjectReference src, Address slot, int markOffset) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(USE_FIELD_BARRIER);
    if (RCHeader.prepareToLogFieldInObject(src)) {
      if (VM.objectModel.isFieldUnlogged(src, markOffset)) {
        ObjectReference tgt = slot.loadObjectReference();
        if (!tgt.isNull())
          decBuffer.push(tgt);
        Word mark = VM.objectModel.markFieldAsLogged(src, markOffset);
        modFieldBuffer.insert(slot, mark.toAddress());
      }
      RCHeader.finishLogging(src);
    }
  }

  private void coalescingFieldWriteBarrierSlow(ObjectReference dst, Offset dstOffset, int bytes) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(USE_FIELD_BARRIER);
    // VM.assertions._assert(false);
    // log each of the to-be-overwritten fields
    if (RCHeader.prepareToLogFieldInObject(dst)) {
      Address cursor = dst.toAddress().plus(dstOffset);
      Address end = cursor.plus(bytes);
      int index = dstOffset.toInt() >> LOG_BYTES_IN_ADDRESS;
      while (cursor.LT(end)) {
        if (VM.objectModel.isFieldUnlogged(dst, index)) {
          decBuffer.push(cursor.loadObjectReference());
          Word mark = VM.objectModel.markFieldAsLogged(dst, index);
          modFieldBuffer.insert(cursor, mark.toAddress());
        }
        cursor = cursor.plus(BYTES_IN_ADDRESS);
        index++;
      }
      RCHeader.finishLogging(dst);
    } else {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(false);
    }
  }

  /****************************************************************************
   *
   * Miscellaneous
   */

  /** @return The active global plan as an <code>RC</code> instance. */
  @Inline
  private static RCBase global() {
    return (RCBase) VM.activePlan.global();
  }
}
