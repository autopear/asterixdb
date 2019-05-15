/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.lsm.common.api;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public interface ILSMIndexOperationContext extends IIndexOperationContext {
    List<ILSMComponent> getComponentHolder();

    List<ILSMDiskComponent> getComponentsToBeMerged();

    ISearchOperationCallback getSearchOperationCallback();

    IExtendedModificationOperationCallback getModificationCallback();

    void setCurrentMutableComponentId(int currentMutableComponentId);

    void setSearchPredicate(ISearchPredicate searchPredicate);

    ISearchPredicate getSearchPredicate();

    List<ILSMDiskComponent> getComponentsToBeReplicated();

    /**
     * @return true if this operation entered the components. Otherwise false.
     */
    boolean isAccessingComponents();

    void setAccessingComponents(boolean accessingComponents);

    PermutingTupleReference getIndexTuple();

    PermutingTupleReference getFilterTuple();

    MultiComparator getFilterCmp();

    /**
     * @return the {@link ILSMIndex} of the component
     */
    ILSMIndex getIndex();

    /**
     * Performance tracing method. Logs the accumulated counters for number of tuples
     *
     * @param tupleCount
     *            the number of tuples represented by the counters
     */
    void logPerformanceCounters(int tupleCount);

    /**
     * Increment the time taken for entering and exiting components
     *
     * @param increment
     *            the time increment in nanoseconds
     */
    void incrementEnterExitTime(long increment);

    /**
     * @return true if performance tracing is enabled, false otherwise
     */
    boolean isTracingEnabled();

    boolean isFilterSkipped();

    void setFilterSkip(boolean skip);

    boolean isRecovery();

    void setRecovery(boolean recovery);

    /**
     * @return the IO operation associated with this context
     */
    ILSMIOOperation getIoOperation();

    /**
     * Set the IO operation associated with this context
     *
     * @param ioOperation
     */
    void setIoOperation(ILSMIOOperation ioOperation);

    /**
     * Set a map in the context to pass pairs of keys and values
     *
     * @param map
     */
    void setParameters(Map<String, Object> map);

    /**
     * @return the key value map of the context
     */
    Map<String, Object> getParameters();

    //    /**
    //     * @return components sorted by freshness (latest first)
    //     * */
    //    static List<ILSMComponent> sortComponents(Map<ILSMComponent, Long> components, boolean isLeveledLSM) {
    //        if (isLeveledLSM) {
    //            Map<Long, ArrayList<ILSMComponent>> toSort = new HashMap<>();
    //            for (ILSMComponent component : components.keySet()) {
    //                ArrayList<ILSMComponent> levelComponents = toSort.getOrDefault(component, new ArrayList<>());
    //                levelComponents.add(component);
    //                toSort.put(components.get(component), levelComponents);
    //            }
    //            List<Long> levels = new ArrayList<>(toSort.keySet());
    //            Collections.sort(levels);
    //            List<ILSMComponent> sorted = new ArrayList<>();
    //            for (int i=0; i<levels.size(); i++) {
    //                ArrayList<ILSMComponent> levelComponents = toSort.get(levels.get(i));
    //                if (levelComponents.size() == 1) {
    //                    sorted.add(0, levelComponents.get(0));
    //                } else {
    //                    Collections.sort(levelComponents, new Comparator<ILSMComponent>() {
    //                        public int compare(ILSMComponent c1, ILSMComponent c2) {
    //                            try {
    //                                long max1 = c1.getId().getMaxId();
    //                                long max2 = c2.getId().getMaxId();
    //                                if (max1 == max2) {
    //                                    return 0;
    //                                } else if (max1 < max2) {
    //                                    return -1;
    //                                } else  {
    //                                    return 1;
    //                                }
    //                            } catch (HyracksDataException ex) {
    //                                return 0;
    //                            }
    //                        }
    //                    });
    //                    for (ILSMComponent component : levelComponents) {
    //                        sorted.add(0, component);
    //                    }
    //                }
    //            }
    //            return sorted;
    //        } else {
    //            List<Map.Entry<ILSMComponent, Long>> list = new LinkedList<>(components.entrySet());
    //            Collections.sort(list, new Comparator<Map.Entry<ILSMComponent, Long> >() {
    //                public int compare(Map.Entry<ILSMComponent, Long> o1, Map.Entry<ILSMComponent, Long> o2) {
    //                    return -(o1.getValue()).compareTo(o2.getValue());
    //                }
    //            });
    //            List<ILSMComponent> sorted = new ArrayList<>();
    //            for (Map.Entry<ILSMComponent, Long> entry : list) {
    //                sorted.add(entry.getKey());
    //            }
    //            return sorted;
    //        }
    //    }
}
