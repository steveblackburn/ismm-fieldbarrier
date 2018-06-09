package org.mmtk.utility;

import org.mmtk.utility.deque.AddressPairDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class FieldMarks {
  @Inline
  public static boolean isFieldUnlogged(ObjectReference src, Word metaData, boolean isArray) {
    if (isArray)
      return VM.objectModel.isRefArrayElementUnlogged(src, metaData.toInt());
    else
      return VM.objectModel.isScalarFieldUnlogged(src, metaData);
  }

  @Inline
  public static void logField(ObjectReference src, Address slot, Word metaData, boolean isArray, AddressPairDeque fieldbuf) {
    if (isArray)
      logRefArrayElement(src, slot, metaData.toInt(), fieldbuf);
    else
      logScalarField(src, slot, metaData, fieldbuf);
  }

  private static void logRefArrayElement(ObjectReference src, Address slot, int index, AddressPairDeque fieldbuf) {
    Address markAddr = VM.objectModel.nonAtomicMarkRefArrayElementAsLogged(src, index);
    fieldbuf.insert(slot, markAddr);
  }

  private static void logScalarField(ObjectReference src, Address slot, Word metaData, AddressPairDeque fieldbuf) {
    Address markAddr = VM.objectModel.nonAtomicMarkScalarFieldAsLogged(src, metaData);
    fieldbuf.insert(slot, markAddr);
  }
}
