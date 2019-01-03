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
package org.jikesrvm.mm.mminterface;

import static org.jikesrvm.runtime.UnboxedSizeConstants.LOG_BYTES_IN_WORD;


/**
 * This class merely exposes the MMTk constants into the Jikes RVM
 * package space so that they can be accessed by the VM in an
 * MM-neutral way.  It is separate from MemoryManager to break
 * cyclic class-loading dependencies.
 */
public class MemoryManagerConstants {
  /** {@code true} if the selected plan needs support for linearly scanning the heap */
  public static final boolean NEEDS_LINEAR_SCAN = Selected.Constraints.get().needsLinearScan();
  /** Number of bits in the GC header required by the selected plan */
  public static final int GC_HEADER_BITS = Selected.Constraints.get().gcHeaderBits();
  /** Number of additional bytes required in the header by the selected plan */
  public static final int GC_HEADER_BYTES = Selected.Constraints.get().gcHeaderWords() << LOG_BYTES_IN_WORD;
  /** {@code true} if the selected plan requires concurrent worker threads */
  public static final boolean NEEDS_CONCURRENT_WORKERS = Selected.Constraints.get().needsConcurrentWorkers();
  /** {@code true} if the selected plan needs support for generating a GC trace */
  public static final boolean GENERATE_GC_TRACE = Selected.Constraints.get().generateGCTrace();
  /** {@code true} if the selected plan may move objects */
  public static final boolean MOVES_OBJECTS = Selected.Constraints.get().movesObjects();
  /** {@code true} if the selected plan moves TIB objects */
  public static final boolean MOVES_TIBS = false;
  /** {@code true} if the selected plan moves code */
  public static final boolean MOVES_CODE = false;
  public static final boolean USE_FIELD_BARRIER_FOR_PUTFIELD = Selected.Constraints.get().useFieldBarrierForPutfield();
  public static final boolean USE_FIELD_BARRIER_FOR_AASTORE = Selected.Constraints.get().useFieldBarrierForAAStore();
  public static final boolean FIELD_BARRIER_PF_USE_GC_BYTE = Selected.Constraints.get().fieldBarrierPFUseGCByte();
  public static final boolean FIELD_BARRIER_AA_USE_GC_BYTE = Selected.Constraints.get().fieldBarrierAAUseGCByte();
  public static final int FIELD_BARRIER_LOWEST_AVAILABLE_BIT = Selected.Constraints.get().fieldBarrierLowestAvailableBit();
  public static final int FIELD_BARRIER_HIGHEST_AVAILABLE_BIT = Selected.Constraints.get().fieldBarrierHighestAvailableBit();
  public static final int FIELD_BARRIER_HEADER_BITS = 1 + (FIELD_BARRIER_HIGHEST_AVAILABLE_BIT - FIELD_BARRIER_LOWEST_AVAILABLE_BIT);
  public static final byte FIELD_BARRIER_HEADER_MASK = (byte) (((1<<FIELD_BARRIER_HEADER_BITS)-1)<<FIELD_BARRIER_LOWEST_AVAILABLE_BIT);
  public static final boolean FIELD_BARRIER_SPACE_EVAL = Selected.Constraints.get().fieldBarrierSpaceEval();
  public static final boolean FIELD_BARRIER_VERBOSE = Selected.Constraints.get().fieldBarrierVerbose();
  public static final int FIELD_BARRIER_AASTORE_THRESHOLD = 0;
}

