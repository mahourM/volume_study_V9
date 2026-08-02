#ifndef VOLUME_STUDY_EXECUTION_TRADING_TYPES_MQH
#define VOLUME_STUDY_EXECUTION_TRADING_TYPES_MQH

enum ENUM_EXECUTION_SIDE
{
   EXECUTION_SIDE_NONE = 0,
   EXECUTION_SIDE_BUY = 1,
   EXECUTION_SIDE_SELL = 2
};

string ExecutionSideToText(const ENUM_EXECUTION_SIDE side)
{
   if(side == EXECUTION_SIDE_BUY)
      return "BUY";
   if(side == EXECUTION_SIDE_SELL)
      return "SELL";
   return "";
}

ENUM_EXECUTION_SIDE ExecutionSideFromText(const string side_text)
{
   string normalized = side_text;
   StringToUpper(normalized);
   if(normalized == "BUY")
      return EXECUTION_SIDE_BUY;
   if(normalized == "SELL")
      return EXECUTION_SIDE_SELL;
   return EXECUTION_SIDE_NONE;
}

ENUM_EXECUTION_SIDE OppositeExecutionSide(const ENUM_EXECUTION_SIDE side)
{
   if(side == EXECUTION_SIDE_BUY)
      return EXECUTION_SIDE_SELL;
   if(side == EXECUTION_SIDE_SELL)
      return EXECUTION_SIDE_BUY;
   return EXECUTION_SIDE_NONE;
}

struct STradingExecutionSignal
{
   string command_type;
<<<<<<< HEAD
   string action;
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   string request_id;
   string position_id;
   string symbol_name;
   string timeframe;
   ENUM_EXECUTION_SIDE side;
   long signal_time_utc_ms;
<<<<<<< HEAD
   long target_entry_open_time_utc_ms;
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   string cluster_id;
   string client_name;
   string client_position_id;
   string client_position_identifier;
   long source_candle_open_time_utc_ms;
   long source_candle_close_time_utc_ms;
   double zone_low;
   double zone_high;
   double stop_reference_price;
   long absorption_candle_time_utc_ms;
   long dominance_candle_time_utc_ms;
   double trigger_bin_price;
   string entry_reason;
   string exit_reason;
};

struct STradingPositionStatusUpdate
{
   string client_name;
   string request_id;
   string position_id;
   string client_position_id;
   string client_position_identifier;
   string symbol_name;
   string timeframe;
   ENUM_EXECUTION_SIDE side;
   string status;
   long signal_time_utc_ms;
   string cluster_id;
   double profit;
   double entry_price;
   long opened_at_utc_ms;
   string rejection_reason;
};

struct SExecutionDecisionRecord
{
   string client_name;
   string timestamp;
   string symbol;
   string timeframe;
   string signal_side;
   string decision_type;
   string decision_result;
   string rejection_reason;
   string failed_execution_stage;
   uint order_check_retcode;
   string order_check_comment;
   uint order_send_retcode;
   string order_send_comment;
   string request_id;
   string position_id;
   string client_position_id;
   string client_position_identifier;
   string execution_comment;
   double entry_price;
   double stop_loss;
   double requested_volume;
   double final_volume;
   double required_margin;
   double free_margin_before;
   double free_margin_after;
   double leverage;
   bool margin_safety_passed;
   string exit_trigger_reason;
   long source_candle_open_time_utc_ms;
   int volume_reduction_step;
   double stop_hit_price;
   int post_stop_delay_seconds;
   double reentry_stop_buffer;
   string broker_time;
};

void InitializeDecisionRecord(
   SExecutionDecisionRecord &record,
   const string client_name,
   const string symbol,
   const string timeframe,
   const ENUM_EXECUTION_SIDE side,
   const string decision_type,
   const string position_id
)
{
   record.client_name = client_name;
   record.timestamp = TimeToString(TimeCurrent(), TIME_DATE | TIME_SECONDS);
   record.symbol = symbol;
   record.timeframe = timeframe;
   record.signal_side = ExecutionSideToText(side);
   record.decision_type = decision_type;
   record.decision_result = "";
   record.rejection_reason = "";
   record.failed_execution_stage = "";
   record.order_check_retcode = 0;
   record.order_check_comment = "";
   record.order_send_retcode = 0;
   record.order_send_comment = "";
   record.request_id = "";
   record.position_id = position_id;
   record.client_position_id = "";
   record.client_position_identifier = "";
   record.execution_comment = "";
   record.entry_price = 0.0;
   record.stop_loss = 0.0;
   record.requested_volume = 0.0;
   record.final_volume = 0.0;
   record.required_margin = 0.0;
   record.free_margin_before = 0.0;
   record.free_margin_after = 0.0;
   record.leverage = 0.0;
   record.margin_safety_passed = false;
   record.exit_trigger_reason = "";
   record.source_candle_open_time_utc_ms = 0;
   record.volume_reduction_step = 0;
   record.stop_hit_price = 0.0;
   record.post_stop_delay_seconds = 0;
   record.reentry_stop_buffer = 0.0;
   record.broker_time = TimeToString(TimeCurrent(), TIME_DATE | TIME_SECONDS);
}

#endif
