package com.hortonworks.dataplane.profilers.worker

case class TableResult(database: String, table: String, column: String, label: String, confidence: Double, status: String)
