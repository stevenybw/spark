/*
 * Author: Bowen Yu <stevenybw@hotmail.com> 2018
 */

package org.apache.spark.shuffle.stream

import org.apache.spark.ShuffleDependency
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.BaseShuffleHandle

private[spark] class StreamShuffleWithoutMergingHandle[K, V](shuffleId: Int,
                                                             numMaps: Int,
                                                             dependency: ShuffleDependency[K, V, V])
extends BaseShuffleHandle(shuffleId, numMaps, dependency) with Logging {
}
