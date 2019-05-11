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
import static org.mmtk.policy.rcimmix.RCImmixConstants.MAX_IMMIX_OBJECT_BYTES;
import static org.mmtk.utility.HeaderByte.FIELD_BARRIER_HIGH_BIT;
import static org.mmtk.utility.HeaderByte.FIELD_BARRIER_LOW_BIT;

import org.mmtk.plan.StopTheWorldConstraints;
import org.vmmagic.pragma.*;

/**
 * This class and its subclasses communicate to the host VM/Runtime
 * any features of the selected plan that it needs to know.  This is
 * separate from the main Plan/PlanLocal class in order to bypass any
 * issues with ordering of static initialization.
 */
@Uninterruptible
public class RCImmixConstraints extends StopTheWorldConstraints {
  @Override
  public int gcHeaderBits() { return 8; }
  @Override
  public int gcHeaderWords() { return 0; }
  @Override
  public boolean needsObjectReferenceWriteBarrier() { return true; }
  @Override
  public boolean objectReferenceBulkCopySupported() { return true; }
  @Override
  public int maxNonLOSDefaultAllocBytes() { return MAX_IMMIX_OBJECT_BYTES;}
  @Override
  public boolean movesObjects() { return true;}
  @Override
  public int maxNonLOSCopyBytes() { return MAX_IMMIX_OBJECT_BYTES; }
  @Override
  public boolean useFieldBarrierForPutfield() { return USE_FIELD_BARRIER_FOR_PUTFIELD; }
  @Override
  public boolean useFieldBarrierForAAStore() { return USE_FIELD_BARRIER_FOR_AASTORE; }
  @Override
  public int fieldBarrierAAStoreThreshold() { return FIELD_BARRIER_AASTORE_THRESHOLD; }
  @Override
  public boolean fieldBarrierUseGCByte() { return FIELD_BARRIER_USE_GC_BYTE; }
  @Override
  public int fieldBarrierLowestAvailableBit() { return FIELD_BARRIER_LOW_BIT; }
  @Override
  public int fieldBarrierHighestAvailableBit() { return FIELD_BARRIER_HIGH_BIT; }
  @Override
  public boolean fieldBarrierSpaceEval() { return FIELD_BARRIER_SPACE_EVAL; }
  @Override
  public boolean fieldBarrierVerbose() { return FIELD_BARRIER_VERBOSE; }
}
