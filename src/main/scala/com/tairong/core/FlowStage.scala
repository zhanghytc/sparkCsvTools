package com.tairong.core

import com.tairong.common.TrContext
import com.tairong.processor.Processor
import com.tairong.reader.Reader
import com.tairong.writer.Writer

import scala.collection.mutable.ArrayBuffer

trait FlowStage extends Serializable {
  var reader: Reader = _
  var processor: Processor = _
  var writer: Writer = _

  def run_(context: TrContext)
}
