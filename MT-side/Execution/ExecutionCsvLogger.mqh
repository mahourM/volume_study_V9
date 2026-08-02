#ifndef VOLUME_STUDY_EXECUTION_CSV_LOGGER_MQH
#define VOLUME_STUDY_EXECUTION_CSV_LOGGER_MQH

#include "TradingTypes.mqh"

class CExecutionCsvLogger
{
private:
   string m_file_name;

   void WriteHeaderIfNeeded(const int handle)
   {
      if(FileSize(handle) > 0)
         return;

      FileWrite(
         handle,
         "client_name",
         "timestamp",
         "symbol",
         "timeframe",
         "signal_side",
         "decision_type",
         "decision_result",
         "rejection_reason",
         "failed_execution_stage",
         "order_check_retcode",
         "order_check_comment",
         "order_send_retcode",
         "order_send_comment",
         "request_id",
         "position_id",
         "client_position_id",
         "client_position_identifier",
         "execution_comment",
         "entry_price",
         "stop_loss",
         "requested_volume",
         "final_volume",
         "required_margin",
         "free_margin_before",
         "free_margin_after",
         "leverage",
         "margin_safety_passed",
         "exit_trigger_reason",
         "source_candle_open_time_utc_ms",
         "volume_reduction_step",
         "stop_hit_price",
         "post_stop_delay_seconds",
         "reentry_stop_buffer",
         "broker_time"
      );
   }

public:
   void Configure(const string file_name)
   {
      m_file_name = file_name;
      if(m_file_name == "")
         m_file_name = "execution_decisions.csv";
   }

   bool RecordDecision(const SExecutionDecisionRecord &record)
   {
      int handle = FileOpen(
         m_file_name,
         FILE_READ | FILE_WRITE | FILE_CSV | FILE_ANSI | FILE_SHARE_READ | FILE_SHARE_WRITE,
         ','
      );
      if(handle == INVALID_HANDLE)
         return false;

      WriteHeaderIfNeeded(handle);
      FileSeek(handle, 0, SEEK_END);
      FileWrite(
         handle,
         record.client_name,
         record.timestamp,
         record.symbol,
         record.timeframe,
         record.signal_side,
         record.decision_type,
         record.decision_result,
         record.rejection_reason,
         record.failed_execution_stage,
         (string)record.order_check_retcode,
         record.order_check_comment,
         (string)record.order_send_retcode,
         record.order_send_comment,
         record.request_id,
         record.position_id,
         record.client_position_id,
         record.client_position_identifier,
         record.execution_comment,
         DoubleToString(record.entry_price, 8),
         DoubleToString(record.stop_loss, 8),
         DoubleToString(record.requested_volume, 8),
         DoubleToString(record.final_volume, 8),
         DoubleToString(record.required_margin, 2),
         DoubleToString(record.free_margin_before, 2),
         DoubleToString(record.free_margin_after, 2),
         DoubleToString(record.leverage, 2),
         record.margin_safety_passed ? "true" : "false",
         record.exit_trigger_reason,
         (string)record.source_candle_open_time_utc_ms,
         (string)record.volume_reduction_step,
         DoubleToString(record.stop_hit_price, 8),
         (string)record.post_stop_delay_seconds,
         DoubleToString(record.reentry_stop_buffer, 8),
         record.broker_time
      );
      FileClose(handle);
      return true;
   }
};

#endif
