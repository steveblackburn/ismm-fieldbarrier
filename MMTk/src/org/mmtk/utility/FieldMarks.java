package org.mmtk.utility;

import org.mmtk.plan.Plan;
import org.mmtk.plan.refcount.RCDecBuffer;
import org.mmtk.utility.deque.AddressPairDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

import static org.mmtk.plan.Plan.*;
import static org.mmtk.plan.Plan.USE_FIELD_BARRIER_FOR_AASTORE;
import static org.mmtk.plan.refcount.RCHeader.BEING_LOGGED;
import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class FieldMarks {

  /* Simple logging field barriers (eg for generational collector) */
  @Inline
  public static void scalarFieldBarrier(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    if (FIELD_BARRIER_PUTFIELD_OOL)
      scalarFieldBarrierOOL(src, slot, metaData, fieldbuf);
    else
      scalarFieldBarrierInline(src, slot, metaData, fieldbuf);
  }

  @Inline
  private static void scalarFieldBarrierInline(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    if (VM.objectModel.isScalarFieldUnlogged(src, metaData)) {
      logScalarFieldOOL(src, slot, metaData, fieldbuf);
    }
  }

  @NoInline
  private static void scalarFieldBarrierOOL(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    if (VM.objectModel.isScalarFieldUnlogged(src, metaData)) {
      logScalarField(src, slot, metaData, fieldbuf);
    }
  }

  @Inline
  public static void refArrayBarrier(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    if (FIELD_BARRIER_AASTORE_OOL)
      refArrayBarrierOOL(src, slot, metaData.toInt(), fieldbuf);
    else
      refArrayBarrierInline(src, slot, metaData.toInt(), fieldbuf);
  }

  @Inline
  private static void refArrayBarrierInline(ObjectReference src, Address slot, int index, AddressPairDeque fieldbuf) {
    if (VM.objectModel.isRefArrayElementUnlogged(src, index))
      logRefArrayElementOOL(src, slot, index, fieldbuf);
  }

  @NoInline
  private static void refArrayBarrierOOL(ObjectReference src, Address slot, int index, AddressPairDeque fieldbuf) {
    if (VM.objectModel.isRefArrayElementUnlogged(src, index))
      logRefArrayElement(src, slot, index, fieldbuf);
  }


  /* Coalescing field barriers, eg for reference counting */
  @Inline
  public static void scalarFieldCoalescingBarrier(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    if (FIELD_BARRIER_PUTFIELD_OOL)
      scalarFieldCoalescingBarrierOOL(src, slot, metaData, fieldbuf, decBuffer);
    else
      scalarFieldCoalescingBarrierInline(src, slot, metaData, fieldbuf, decBuffer);
  }

  @Inline
  private static void scalarFieldCoalescingBarrierInline(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    if (VM.objectModel.isScalarFieldUnlogged(src, metaData)) {
      logScalarFieldCoalescingOOL(src, slot, metaData, fieldbuf, decBuffer);
    }
  }

  @NoInline
  private static void scalarFieldCoalescingBarrierOOL(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    if (VM.objectModel.isScalarFieldUnlogged(src, metaData)) {
      logFieldCoalescing(src, slot, metaData, false, fieldbuf, decBuffer);
    }
  }

  @NoInline
  private static void logScalarFieldCoalescingOOL(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    logFieldCoalescing(src, slot, metaData, false, fieldbuf, decBuffer);
  }

  @Inline
  public static void refArrayCoalescingBarrier(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    if (FIELD_BARRIER_AASTORE_OOL)
      refArrayCoalescingBarrierOOL(src, slot, metaData.toInt(), fieldbuf, decBuffer);
    else
      refArrayCoalescingBarrierInline(src, slot, metaData.toInt(), fieldbuf, decBuffer);
  }

  @Inline
  private static void refArrayCoalescingBarrierInline(ObjectReference src, Address slot, int index, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    if (VM.objectModel.isRefArrayElementUnlogged(src, index))
      logRefArrayElementCoalescingOOL(src, slot, index, fieldbuf, decBuffer);
  }

  @NoInline
  private static void refArrayCoalescingBarrierOOL(ObjectReference src, Address slot, int index, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    if (VM.objectModel.isRefArrayElementUnlogged(src, index))
      logRefArrayElementCoalescingOOL(src, slot, index, fieldbuf, decBuffer);
  }

  @NoInline
  private static void logRefArrayElementCoalescingOOL(ObjectReference src, Address slot, int index, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    logFieldCoalescing(src, slot, Word.fromIntSignExtend(index), true, fieldbuf, decBuffer);
  }

  @Inline
  @Uninterruptible
  private static boolean prepareToLogFieldInObject(ObjectReference object) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(USE_FIELD_BARRIER_FOR_PUTFIELD || USE_FIELD_BARRIER_FOR_AASTORE);
    Word oldValue;
    do {
      oldValue = VM.objectModel.prepareAvailableBits(object);
    } while ((oldValue.and(BEING_LOGGED).EQ(BEING_LOGGED)) ||
            !VM.objectModel.attemptAvailableBits(object, oldValue, oldValue.or(BEING_LOGGED)));
    if (VM.VERIFY_ASSERTIONS) {
      Word value = VM.objectModel.readAvailableBitsWord(object);
      VM.assertions._assert(value.and(BEING_LOGGED).EQ(BEING_LOGGED));
    }
    return true;
  }

  @Inline
  @Uninterruptible
  private static void finishLogging(ObjectReference object) {
    Word value = VM.objectModel.readAvailableBitsWord(object);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(value.and(BEING_LOGGED).EQ(BEING_LOGGED));
    VM.objectModel.writeAvailableBitsWord(object, value.and(BEING_LOGGED.not()));
  }

  @Inline
  private static void logFieldCoalescing(ObjectReference src, Address slot, Word metaData, boolean isArray, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    if (VM.VERIFY_ASSERTIONS)
      VM.assertions._assert((USE_FIELD_BARRIER_FOR_PUTFIELD && !isArray) || (USE_FIELD_BARRIER_FOR_AASTORE && isArray));
    if (FIELD_BARRIER_STATS) Plan.slow.inc();

    if (FIELD_BARRIER_ARRAY_QUANTUM == 1) {
      int wordOffset = isArray ? VM.objectModel.wordOffsetFromIndex(metaData.toInt()) : VM.objectModel.wordOffsetFromMetadata(metaData);
      Address markAddress = src.toAddress().plus(wordOffset);
      Word bitmask = isArray ? VM.objectModel.bitMaskFromIndex(metaData.toInt()) : VM.objectModel.bitMaskFromMetadata(metaData);
      ObjectReference oldReference = slot.loadObjectReference();

      /* race to set field mark bit */
      Word oldMarks;
      do {
        oldMarks = markAddress.prepareWord();
        if (oldMarks.and(bitmask).isZero())
          return; // field already logged (or being logged); must not create log entry
      } while (!markAddress.attempt(oldMarks, oldMarks.xor(bitmask)));

      /* won race, so we are first to be updating this field, so log info */
      if (!oldReference.isNull()) {
        if (FIELD_BARRIER_STATS) Plan.wordsLogged.inc();
        decBuffer.push(oldReference);
      }
      if (FIELD_BARRIER_STATS) Plan.wordsLogged.inc(2);
      fieldbuf.insert(slot, markAddress);
    } else {
      if (prepareToLogFieldInObject(src)) {
        if (isFieldUnlogged(src, metaData, isArray)) {
          if (isArray && FIELD_BARRIER_ARRAY_QUANTUM > 1)
            logArraySlots(src, metaData, fieldbuf, decBuffer);
          else
            logSlot(src, slot, metaData, isArray, fieldbuf, decBuffer);
        }
        finishLogging(src);
      }
    }
  }

  @Inline
  private static void logSlot(ObjectReference src, Address slot, Word metaData, boolean isArray, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    ObjectReference tgt = slot.loadObjectReference();
    if (!tgt.isNull()) {
      if (FIELD_BARRIER_STATS) Plan.wordsLogged.inc();
      decBuffer.push(tgt);
    }
    Address markAddr = isArray ? VM.objectModel.nonAtomicMarkRefArrayElementAsLogged(src, metaData.toInt()) : VM.objectModel.nonAtomicMarkScalarFieldAsLogged(src, metaData);
    if (FIELD_BARRIER_STATS) Plan.wordsLogged.inc(2);
    fieldbuf.insert(slot, markAddr);
  }

  @Inline
  private static void logArraySlots(ObjectReference src, Word metaData, AddressPairDeque fieldbuf, RCDecBuffer decBuffer) {
    int index = metaData.toInt();
    int len = VM.objectModel.getArrayLength(src);
    int firstidx = index & ~(FIELD_BARRIER_ARRAY_QUANTUM -1);
    int lastidx = firstidx + (FIELD_BARRIER_ARRAY_QUANTUM -1);
    if (lastidx >= len - 1)
      lastidx = len - 1;
    Address cursor = src.toAddress().plus(firstidx<<LOG_BYTES_IN_ADDRESS);
    Address sentinel =src.toAddress().plus(lastidx<<LOG_BYTES_IN_ADDRESS);

    while (cursor.LE(sentinel)) {

      ObjectReference tgt = cursor.loadObjectReference();
      if (!tgt.isNull()) {
        if (FIELD_BARRIER_STATS) Plan.wordsLogged.inc();
        decBuffer.push(tgt);
      }
      cursor = cursor.plus(BYTES_IN_ADDRESS);
    }

    Address markAddr = VM.objectModel.nonAtomicMarkRefArrayElementAsLogged(src, metaData.toInt());
    if (FIELD_BARRIER_STATS) Plan.wordsLogged.inc(2);
    Address encodedSlot = encodeSlot(src.toAddress().plus(firstidx<<LOG_BYTES_IN_ADDRESS), markAddr, 1 + lastidx - firstidx);
    fieldbuf.insert(encodedSlot, markAddr);
  }

  private static final int COUNT_SHIFT = BITS_IN_ADDRESS-8;
  private static final Word MASK = Word.fromIntSignExtend((1<<COUNT_SHIFT)-1);

  public static int fieldMarksRequired(int numElements) {
    if (FIELD_BARRIER_ARRAY_QUANTUM == 1)
      return numElements;
    else
      return (numElements + FIELD_BARRIER_ARRAY_QUANTUM - 1)>>LOG_FIELD_BARRIER_ARRAY_QUANTUM;
  }

  private static Address encodeSlot(Address slot, Address markAddr, int slots) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(slots < 256);
    Address rtn = slot.toWord().and(MASK).or(Word.fromIntZeroExtend(slots).lsh(COUNT_SHIFT)).or(Word.one()).toAddress();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(slot == extractSlot(rtn, markAddr));
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(slots == extractSlotCount(rtn));
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isMultiSlot(rtn));
    return rtn;
  }

  public static Address extractSlot(Address encoded, Address markAddress) {
    Word hi = markAddress.toWord().and(MASK.not());
    Word lo = encoded.toWord().and(MASK);

    return hi.or(lo).and(Word.one().not()).toAddress();
  }

  public static int extractSlotCount(Address encoded) {
    return encoded.toWord().rshl(COUNT_SHIFT).toInt();
  }

  public static boolean isMultiSlot(Address encoded) {
    return encoded.toWord().and(Word.one()).EQ(Word.one());
  }

  @Inline
  public static boolean isFieldUnlogged(ObjectReference src, Word metaData, boolean isArray) {
    if (isArray)
      return VM.objectModel.isRefArrayElementUnlogged(src, metaData.toInt());
    else
      return VM.objectModel.isScalarFieldUnlogged(src, metaData);
  }

  @Inline
  private static void logRefArrayElement(ObjectReference src, Address slot, int index, AddressPairDeque fieldbuf) {
    Address markAddr = VM.objectModel.nonAtomicMarkRefArrayElementAsLogged(src, index);
    if (FIELD_BARRIER_STATS) Plan.wordsLogged.inc();
    if (FIELD_BARRIER_STATS) Plan.slow.inc();
    fieldbuf.insert(slot, markAddr);
  }

  @NoInline
  private static void logRefArrayElementOOL(ObjectReference src, Address slot, int index, AddressPairDeque fieldbuf) {
    logRefArrayElement(src, slot, index, fieldbuf);
  }

  @Inline
  public static void logScalarField(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    Address markAddr = VM.objectModel.nonAtomicMarkScalarFieldAsLogged(src, metaData);
    if (FIELD_BARRIER_STATS) Plan.wordsLogged.inc();
    if (FIELD_BARRIER_STATS) Plan.slow.inc();
    fieldbuf.insert(slot, markAddr);
  }

  @NoInline
  public static void logScalarFieldOOL(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    logScalarField(src, slot, metaData, fieldbuf);
  }
}
