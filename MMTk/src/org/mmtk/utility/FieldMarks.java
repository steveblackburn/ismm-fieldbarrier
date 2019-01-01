package org.mmtk.utility;

import org.mmtk.utility.deque.AddressPairDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

import static org.mmtk.plan.Plan.FIELD_BARRIER_AASTORE_OOL;
import static org.mmtk.plan.Plan.FIELD_BARRIER_PUTFIELD_OOL;

@Uninterruptible
public class FieldMarks {
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
      refArrayBarrierOOL(src, slot, metaData, fieldbuf);
    else
      refArrayBarrierInline(src, slot, metaData, fieldbuf);
  }

  @Inline
  private static void refArrayBarrierInline(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    if (isRefArrayElementlogged(src, metaData))
      logRefArrayElementOOL(src, slot, metaData, fieldbuf);
  }

  @NoInline
  private static void refArrayBarrierOOL(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    if (isRefArrayElementlogged(src, metaData))
      logRefArrayElement(src, slot, metaData, fieldbuf);
  }


  @Inline
  public static boolean isFieldUnlogged(ObjectReference src, Word metaData, boolean isArray) {
    if (isArray)
      return VM.objectModel.isRefArrayElementUnlogged(src, metaData.toInt());
    else
      return VM.objectModel.isScalarFieldUnlogged(src, metaData);
  }

  @Inline
  public static boolean isScalarFieldUnlogged(ObjectReference src, Word metaData) {
    return VM.objectModel.isScalarFieldUnlogged(src, metaData);
  }

  @Inline
  public static boolean isRefArrayElementlogged(ObjectReference src, Word metaData) {
    return VM.objectModel.isRefArrayElementUnlogged(src, metaData.toInt());
  }

  @Inline
  public static void logRefArrayElement(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    int index = metaData.toInt();
    Address markAddr = VM.objectModel.nonAtomicMarkRefArrayElementAsLogged(src, index);
    fieldbuf.insert(slot, markAddr);
  }

  @NoInline
  public static void logRefArrayElementOOL(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    logRefArrayElement(src, slot, metaData, fieldbuf);
  }

  @Inline
  public static void logScalarField(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    Address markAddr = VM.objectModel.nonAtomicMarkScalarFieldAsLogged(src, metaData);
    fieldbuf.insert(slot, markAddr);
  }

  @NoInline
  public static void logScalarFieldOOL(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    logScalarField(src, slot, metaData, fieldbuf);
  }
}
