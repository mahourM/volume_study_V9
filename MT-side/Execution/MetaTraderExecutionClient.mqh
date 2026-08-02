#ifndef VOLUME_STUDY_METATRADER_EXECUTION_CLIENT_MQH
#define VOLUME_STUDY_METATRADER_EXECUTION_CLIENT_MQH

#include <Trade/Trade.mqh>
#include "ExecutionCsvLogger.mqh"
#include "IExecutionClient.mqh"

class CMetaTraderExecutionClient : public IExecutionClient
{
private:
   CExecutionCsvLogger *m_logger;
   double m_risk_percent;
   double m_preferred_leverage;
   ulong m_magic_number;
   int m_deviation_points;
   int m_post_stop_reentry_delay_seconds;
   int m_reentry_stop_buffer_atr_period;
   double m_reentry_stop_buffer_atr_multiplier;
   string m_post_stop_symbols[];
   int m_post_stop_sides[];
   double m_post_stop_hit_prices[];
   datetime m_post_stop_times[];

   ENUM_TIMEFRAMES TradingPeriodFromText(const string timeframe) const
   {
      if(timeframe == "M1")
         return PERIOD_M1;
      if(timeframe == "M5")
         return PERIOD_M5;
      if(timeframe == "M15")
         return PERIOD_M15;
      if(timeframe == "M30")
         return PERIOD_M30;
      if(timeframe == "H1")
         return PERIOD_H1;
      if(timeframe == "H4")
         return PERIOD_H4;
      return PERIOD_CURRENT;
   }

   double NormalizeSymbolPrice(const string symbol, const double price) const
   {
      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
      return NormalizeDouble(price, digits);
   }

   datetime UtcMsToServerTime(const long utc_ms) const
   {
      return (datetime)((utc_ms / 1000) + (TimeCurrent() - TimeGMT()));
   }

   bool StopLossBehindEntry(
      const ENUM_EXECUTION_SIDE side,
      const double entry_price,
      const double stop_loss
   ) const
   {
      if(entry_price <= 0.0 || stop_loss <= 0.0)
         return false;
      if(side == EXECUTION_SIDE_BUY)
         return (stop_loss < entry_price);
      if(side == EXECUTION_SIDE_SELL)
         return (stop_loss > entry_price);
      return false;
   }

   string CanonicalPositionId(const string client_position_id) const
   {
      if(client_position_id == "")
         return "";
      return "MT5:" + client_position_id;
   }

   string ShortSymbolForComment(const string symbol) const
   {
      return StringSubstr(symbol, 0, 12);
   }

   string BuildExecutionComment(const STradingExecutionSignal &signal) const
   {
      datetime comment_time = TimeGMT();
      if(signal.signal_time_utc_ms > 0)
         comment_time = (datetime)(signal.signal_time_utc_ms / 1000);

      MqlDateTime parts;
      TimeToStruct(comment_time, parts);
      string side_text = (signal.side == EXECUTION_SIDE_BUY) ? "B" : "S";
      string comment = StringFormat(
         "%s-%s-%02d%02d%02d",
         ShortSymbolForComment(signal.symbol_name),
         side_text,
         parts.hour,
         parts.min,
         parts.sec
      );
      return StringSubstr(comment, 0, 31);
   }

   bool PositionMatchesClientId(
      const ulong ticket,
      const string client_position_id,
      const string client_position_identifier
   ) const
   {
      if(client_position_id != "" && client_position_id == IntegerToString((long)ticket))
         return true;
      long position_identifier = PositionGetInteger(POSITION_IDENTIFIER);
      if(client_position_id != "" && client_position_id == IntegerToString(position_identifier))
         return true;
      return (client_position_identifier != "" && client_position_identifier == IntegerToString(position_identifier));
   }

   bool IsOwnPositionFallbackMatch() const
   {
      long magic = PositionGetInteger(POSITION_MAGIC);
      return (magic == (long)m_magic_number);
   }

   bool CalculateZoneHeightFallbackStopLoss(
      const STradingExecutionSignal &signal,
      const double entry_price,
      double &stop_loss
   ) const
   {
      double zone_low = MathMin(signal.zone_low, signal.zone_high);
      double zone_high = MathMax(signal.zone_low, signal.zone_high);
      double zone_height = zone_high - zone_low;
      if(zone_height <= 0.0)
         return false;

      if(signal.side == EXECUTION_SIDE_BUY)
         stop_loss = entry_price - zone_height;
      else if(signal.side == EXECUTION_SIDE_SELL)
         stop_loss = entry_price + zone_height;
      else
         return false;

      stop_loss = NormalizeSymbolPrice(signal.symbol_name, stop_loss);
      return StopLossBehindEntry(signal.side, entry_price, stop_loss);
   }

   int FindPostStopRecordIndex(const string symbol, const ENUM_EXECUTION_SIDE side) const
   {
      for(int i = 0; i < ArraySize(m_post_stop_symbols); i++)
      {
         if(m_post_stop_symbols[i] == symbol && m_post_stop_sides[i] == (int)side)
            return i;
      }
      return -1;
   }

   void RemovePostStopRecordAt(const int index)
   {
      int count = ArraySize(m_post_stop_symbols);
      if(index < 0 || index >= count)
         return;
      for(int i = index + 1; i < count; i++)
      {
         m_post_stop_symbols[i - 1] = m_post_stop_symbols[i];
         m_post_stop_sides[i - 1] = m_post_stop_sides[i];
         m_post_stop_hit_prices[i - 1] = m_post_stop_hit_prices[i];
         m_post_stop_times[i - 1] = m_post_stop_times[i];
      }
      ArrayResize(m_post_stop_symbols, count - 1);
      ArrayResize(m_post_stop_sides, count - 1);
      ArrayResize(m_post_stop_hit_prices, count - 1);
      ArrayResize(m_post_stop_times, count - 1);
   }

   bool CalculateManualAtr(
      const string symbol,
      const ENUM_TIMEFRAMES period,
      const int atr_period,
      double &atr_value
   ) const
   {
      atr_value = 0.0;
      if(atr_period <= 0)
         return false;
      if(Bars(symbol, period) < atr_period + 2)
         return false;

      double true_range_sum = 0.0;
      for(int shift = 1; shift <= atr_period; shift++)
      {
         double high_price = iHigh(symbol, period, shift);
         double low_price = iLow(symbol, period, shift);
         double previous_close = iClose(symbol, period, shift + 1);
         if(high_price <= 0.0 || low_price <= 0.0 || previous_close <= 0.0)
            return false;
         double high_low = high_price - low_price;
         double high_previous_close = MathAbs(high_price - previous_close);
         double low_previous_close = MathAbs(low_price - previous_close);
         true_range_sum += MathMax(high_low, MathMax(high_previous_close, low_previous_close));
      }

      atr_value = true_range_sum / atr_period;
      return (atr_value > 0.0);
   }

   double NormalizeVolumeDown(const string symbol, const double volume) const
   {
      double min_volume = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
      double max_volume = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
      double step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
      if(step <= 0.0)
         step = min_volume;
      if(step <= 0.0)
         return 0.0;

      double bounded_volume = MathMin(volume, max_volume);
      double steps = MathFloor((bounded_volume + 0.0000000001) / step);
      double normalized = steps * step;
      if(normalized < min_volume)
         return 0.0;
      return NormalizeDouble(normalized, 8);
   }

   double PreviousVolumeStep(const string symbol, const double volume) const
   {
      double step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
      if(step <= 0.0)
         step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
      if(step <= 0.0)
         return 0.0;
      return NormalizeVolumeDown(symbol, volume - step);
   }

   bool CalculateEntryPrice(const string symbol, const ENUM_EXECUTION_SIDE side, double &entry_price) const
   {
      MqlTick tick;
      if(!SymbolInfoTick(symbol, tick))
         return false;
      entry_price = (side == EXECUTION_SIDE_BUY) ? tick.ask : tick.bid;
      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
      entry_price = NormalizeDouble(entry_price, digits);
      return (entry_price > 0.0);
   }

   bool CalculateSourceStopLoss(const STradingExecutionSignal &signal, double &stop_loss, long &source_open_utc_ms) const
   {
      if(signal.source_candle_open_time_utc_ms <= 0)
         return false;
      datetime source_time = UtcMsToServerTime(signal.source_candle_open_time_utc_ms);
      ENUM_TIMEFRAMES period = TradingPeriodFromText(signal.timeframe);
      int shift = iBarShift(signal.symbol_name, period, source_time, true);
      if(shift < 0)
         return false;

      datetime candle_open = iTime(signal.symbol_name, period, shift);
      if(candle_open <= 0 || candle_open != source_time)
         return false;

      source_open_utc_ms = signal.source_candle_open_time_utc_ms;
      if(signal.side == EXECUTION_SIDE_BUY)
         stop_loss = iLow(signal.symbol_name, period, shift);
      else if(signal.side == EXECUTION_SIDE_SELL)
         stop_loss = iHigh(signal.symbol_name, period, shift);
      else
         return false;

      stop_loss = NormalizeSymbolPrice(signal.symbol_name, stop_loss);
      return (stop_loss > 0.0);
   }

   bool CalculatePostStopStopLoss(
      const STradingExecutionSignal &signal,
      double &stop_loss,
      double &reentry_stop_buffer
   ) const
   {
      reentry_stop_buffer = 0.0;
      int record_index = FindPostStopRecordIndex(signal.symbol_name, signal.side);
      if(record_index < 0)
         return false;

      double atr_value = 0.0;
      if(!CalculateManualAtr(signal.symbol_name, TradingPeriodFromText(signal.timeframe), m_reentry_stop_buffer_atr_period, atr_value))
         return false;

      reentry_stop_buffer = atr_value * m_reentry_stop_buffer_atr_multiplier;
      if(reentry_stop_buffer <= 0.0)
         return false;

      double stop_hit_price = m_post_stop_hit_prices[record_index];
      if(signal.side == EXECUTION_SIDE_BUY)
         stop_loss = stop_hit_price - reentry_stop_buffer;
      else if(signal.side == EXECUTION_SIDE_SELL)
         stop_loss = stop_hit_price + reentry_stop_buffer;
      else
         return false;

      stop_loss = NormalizeSymbolPrice(signal.symbol_name, stop_loss);
      reentry_stop_buffer = NormalizeSymbolPrice(signal.symbol_name, reentry_stop_buffer);
      return (stop_loss > 0.0);
   }

   bool CalculateLossForVolume(
      const string symbol,
      const ENUM_EXECUTION_SIDE side,
      const double volume,
      const double entry_price,
      const double stop_loss,
      double &loss_amount
   ) const
   {
      if(!StopLossBehindEntry(side, entry_price, stop_loss))
         return false;
      ENUM_ORDER_TYPE order_type = (side == EXECUTION_SIDE_BUY) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
      double calculated_profit = 0.0;
      if(!OrderCalcProfit(order_type, symbol, volume, entry_price, stop_loss, calculated_profit))
         return false;
      loss_amount = MathAbs(calculated_profit);
      return (loss_amount > 0.0);
   }

   bool ValidateMarginAndSafety(
      const STradingExecutionSignal &signal,
      const double entry_price,
      const double stop_loss,
      const double volume,
      double &required_margin,
      double &free_margin_before,
      double &free_margin_after,
      bool &margin_safety_passed
   ) const
   {
      margin_safety_passed = false;
      free_margin_before = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
      ENUM_ORDER_TYPE order_type = (signal.side == EXECUTION_SIDE_BUY) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
      if(!OrderCalcMargin(order_type, signal.symbol_name, volume, entry_price, required_margin))
         return false;
      free_margin_after = free_margin_before - required_margin;
      if(free_margin_after <= 0.0)
         return false;

      double potential_loss = 0.0;
      if(!CalculateLossForVolume(signal.symbol_name, signal.side, volume, entry_price, stop_loss, potential_loss))
         return false;

      margin_safety_passed = (free_margin_after > potential_loss);
      return margin_safety_passed;
   }

   ENUM_ORDER_TYPE_FILLING ResolveMarketOrderFilling(const string symbol) const
   {
      int filling_mode = (int)SymbolInfoInteger(symbol, SYMBOL_FILLING_MODE);
      if((filling_mode & SYMBOL_FILLING_FOK) == SYMBOL_FILLING_FOK)
         return ORDER_FILLING_FOK;
      if((filling_mode & SYMBOL_FILLING_IOC) == SYMBOL_FILLING_IOC)
         return ORDER_FILLING_IOC;
      return ORDER_FILLING_RETURN;
   }

   void BuildMarketOrderRequest(
      const STradingExecutionSignal &signal,
      const double entry_price,
      const double stop_loss,
      const double volume,
      MqlTradeRequest &request
   ) const
   {
      ZeroMemory(request);
      request.action = TRADE_ACTION_DEAL;
      request.symbol = signal.symbol_name;
      request.volume = volume;
      request.type = (signal.side == EXECUTION_SIDE_BUY) ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
      request.price = entry_price;
      request.sl = stop_loss;
      request.deviation = m_deviation_points;
      request.magic = m_magic_number;
      request.comment = BuildExecutionComment(signal);
      request.type_time = ORDER_TIME_GTC;
      request.type_filling = ResolveMarketOrderFilling(signal.symbol_name);
   }

   bool BrokerWouldAcceptOrder(
      const STradingExecutionSignal &signal,
      const double entry_price,
      const double stop_loss,
      const double volume,
      double &check_margin,
      uint &check_retcode,
      string &check_comment
   ) const
   {
      MqlTradeRequest request;
      MqlTradeCheckResult check_result;
      ZeroMemory(check_result);
      check_margin = 0.0;
      check_retcode = 0;
      check_comment = "";

      BuildMarketOrderRequest(signal, entry_price, stop_loss, volume, request);

      if(!OrderCheck(request, check_result))
      {
         check_retcode = check_result.retcode;
         check_comment = StringFormat(
            "OrderCheck call failed; last_error=%d; comment=%s",
            GetLastError(),
            check_result.comment
         );
         return false;
      }
      check_margin = check_result.margin;
      check_retcode = check_result.retcode;
      check_comment = check_result.comment;
      return true;
   }

   bool SendMarketOrder(
      const STradingExecutionSignal &signal,
      const double entry_price,
      const double stop_loss,
      const double volume,
      uint &send_retcode,
      string &send_comment
   ) const
   {
      MqlTradeRequest request;
      MqlTradeResult result;
      ZeroMemory(result);
      send_retcode = 0;
      send_comment = "";

      BuildMarketOrderRequest(signal, entry_price, stop_loss, volume, request);

      if(!OrderSend(request, result))
      {
         send_retcode = result.retcode;
         send_comment = StringFormat(
            "OrderSend call failed; last_error=%d; comment=%s",
            GetLastError(),
            result.comment
         );
         return false;
      }
      send_retcode = result.retcode;
      send_comment = result.comment;
      return (
         result.retcode == TRADE_RETCODE_DONE ||
         result.retcode == TRADE_RETCODE_PLACED ||
         result.retcode == TRADE_RETCODE_DONE_PARTIAL
      );
   }

public:
   CMetaTraderExecutionClient()
   {
      m_logger = NULL;
      m_risk_percent = 3.0;
      m_preferred_leverage = 50.0;
      m_magic_number = 7009001;
      m_deviation_points = 20;
      m_post_stop_reentry_delay_seconds = 30;
      m_reentry_stop_buffer_atr_period = 14;
      m_reentry_stop_buffer_atr_multiplier = 0.10;
   }

   void Configure(
      CExecutionCsvLogger *logger,
      const double risk_percent,
      const double preferred_leverage,
      const ulong magic_number,
      const int deviation_points,
      const int post_stop_reentry_delay_seconds,
      const int reentry_stop_buffer_atr_period,
      const double reentry_stop_buffer_atr_multiplier
   )
   {
      m_logger = logger;
      m_risk_percent = risk_percent;
      m_preferred_leverage = preferred_leverage;
      m_magic_number = magic_number;
      m_deviation_points = deviation_points;
      m_post_stop_reentry_delay_seconds = post_stop_reentry_delay_seconds > 0 ? post_stop_reentry_delay_seconds : 30;
      m_reentry_stop_buffer_atr_period = reentry_stop_buffer_atr_period > 0 ? reentry_stop_buffer_atr_period : 14;
      m_reentry_stop_buffer_atr_multiplier = reentry_stop_buffer_atr_multiplier > 0.0 ? reentry_stop_buffer_atr_multiplier : 0.10;
   }

   virtual string ClientName()
   {
      return "metatrader";
   }

   void RegisterStopLossClosure(
      const string symbol,
      const string timeframe,
      const ENUM_EXECUTION_SIDE side,
      const double stop_hit_price,
      const datetime broker_time
   )
   {
      if(symbol == "" || side == EXECUTION_SIDE_NONE || stop_hit_price <= 0.0)
         return;

      int index = FindPostStopRecordIndex(symbol, side);
      if(index < 0)
      {
         index = ArraySize(m_post_stop_symbols);
         ArrayResize(m_post_stop_symbols, index + 1);
         ArrayResize(m_post_stop_sides, index + 1);
         ArrayResize(m_post_stop_hit_prices, index + 1);
         ArrayResize(m_post_stop_times, index + 1);
      }

      m_post_stop_symbols[index] = symbol;
      m_post_stop_sides[index] = (int)side;
      m_post_stop_hit_prices[index] = NormalizeSymbolPrice(symbol, stop_hit_price);
      m_post_stop_times[index] = broker_time;

      if(m_logger != NULL)
      {
         SExecutionDecisionRecord record;
         InitializeDecisionRecord(record, ClientName(), symbol, timeframe, side, "stop_loss_hit", "");
         record.decision_result = "approved";
         record.stop_hit_price = m_post_stop_hit_prices[index];
         record.post_stop_delay_seconds = m_post_stop_reentry_delay_seconds;
         record.broker_time = TimeToString(broker_time, TIME_DATE | TIME_SECONDS);
         m_logger.RecordDecision(record);
      }
   }

   bool IsPostStopDelayActive(const STradingExecutionSignal &signal, int &remaining_seconds) const
   {
      remaining_seconds = 0;
      int index = FindPostStopRecordIndex(signal.symbol_name, signal.side);
      if(index < 0)
         return false;

      int elapsed_seconds = (int)(TimeCurrent() - m_post_stop_times[index]);
      remaining_seconds = m_post_stop_reentry_delay_seconds - elapsed_seconds;
      return (remaining_seconds > 0);
   }

   virtual bool HasOpenPosition(const string symbol, const ENUM_EXECUTION_SIDE side)
   {
      for(int i = 0; i < PositionsTotal(); i++)
      {
         ulong ticket = PositionGetTicket(i);
         if(ticket == 0 || !PositionSelectByTicket(ticket))
            continue;
         if(PositionGetString(POSITION_SYMBOL) != symbol)
            continue;
         long position_type = PositionGetInteger(POSITION_TYPE);
         if(side == EXECUTION_SIDE_BUY && position_type == POSITION_TYPE_BUY)
            return true;
         if(side == EXECUTION_SIDE_SELL && position_type == POSITION_TYPE_SELL)
            return true;
      }
      return false;
   }

   virtual bool HasAnyOpenPosition(const string symbol, ENUM_EXECUTION_SIDE &side_out, double &profit_out, string &position_id_out)
   {
      side_out = EXECUTION_SIDE_NONE;
      profit_out = 0.0;
      position_id_out = "";
      for(int i = 0; i < PositionsTotal(); i++)
      {
         ulong ticket = PositionGetTicket(i);
         if(ticket == 0 || !PositionSelectByTicket(ticket))
            continue;
         if(PositionGetString(POSITION_SYMBOL) != symbol)
            continue;
         long position_type = PositionGetInteger(POSITION_TYPE);
         side_out = (position_type == POSITION_TYPE_BUY) ? EXECUTION_SIDE_BUY : EXECUTION_SIDE_SELL;
         profit_out = PositionGetDouble(POSITION_PROFIT);
         position_id_out = CanonicalPositionId(IntegerToString((long)ticket));
         return true;
      }
      return false;
   }

   bool FindOpenPositionDetails(
      const string position_id,
      const string symbol,
      const ENUM_EXECUTION_SIDE side,
      string &position_id_out,
      string &client_position_id_out,
      string &client_position_identifier_out,
      string &execution_comment_out,
      double &profit_out,
      double &entry_price_out,
      long &opened_at_utc_ms_out
   )
   {
      position_id_out = "";
      client_position_id_out = "";
      client_position_identifier_out = "";
      execution_comment_out = "";
      profit_out = 0.0;
      entry_price_out = 0.0;
      opened_at_utc_ms_out = 0;
      for(int i = 0; i < PositionsTotal(); i++)
      {
         ulong ticket = PositionGetTicket(i);
         if(ticket == 0 || !PositionSelectByTicket(ticket))
            continue;
         if(PositionGetString(POSITION_SYMBOL) != symbol)
            continue;
         long position_type = PositionGetInteger(POSITION_TYPE);
         if(side == EXECUTION_SIDE_BUY && position_type != POSITION_TYPE_BUY)
            continue;
         if(side == EXECUTION_SIDE_SELL && position_type != POSITION_TYPE_SELL)
            continue;
         if(StringFind(position_id, "MT5:") == 0 && position_id != CanonicalPositionId(IntegerToString((long)ticket)))
            continue;

         client_position_id_out = IntegerToString((long)ticket);
         client_position_identifier_out = IntegerToString(PositionGetInteger(POSITION_IDENTIFIER));
         position_id_out = CanonicalPositionId(client_position_id_out);
         execution_comment_out = PositionGetString(POSITION_COMMENT);
         profit_out = PositionGetDouble(POSITION_PROFIT);
         entry_price_out = PositionGetDouble(POSITION_PRICE_OPEN);
         opened_at_utc_ms_out = ((long)PositionGetInteger(POSITION_TIME)) * 1000;
         return true;
      }
      return false;
   }

   virtual bool OpenPosition(const STradingExecutionSignal &signal, SExecutionDecisionRecord &record)
   {
      InitializeDecisionRecord(record, ClientName(), signal.symbol_name, signal.timeframe, signal.side, "entry", signal.position_id);
      record.request_id = signal.request_id;
      record.execution_comment = BuildExecutionComment(signal);
      record.leverage = m_preferred_leverage;

      ENUM_EXECUTION_SIDE existing_side = EXECUTION_SIDE_NONE;
      double existing_profit = 0.0;
      string existing_position_id = "";
      if(HasAnyOpenPosition(signal.symbol_name, existing_side, existing_profit, existing_position_id))
      {
         record.decision_result = "rejected";
         record.rejection_reason = "SAME_SYMBOL_POSITION_EXISTS";
         record.failed_execution_stage = "open_position_guard";
         return false;
      }

      int post_stop_remaining_seconds = 0;
      if(IsPostStopDelayActive(signal, post_stop_remaining_seconds))
      {
         record.decision_result = "rejected";
         record.rejection_reason = "POST_STOP_REENTRY_DELAY_ACTIVE";
         record.failed_execution_stage = "post_stop_reentry_delay";
         record.post_stop_delay_seconds = post_stop_remaining_seconds;
         int stop_record_index = FindPostStopRecordIndex(signal.symbol_name, signal.side);
         if(stop_record_index >= 0)
            record.stop_hit_price = m_post_stop_hit_prices[stop_record_index];
         return false;
      }

      double entry_price = 0.0;
      if(!CalculateEntryPrice(signal.symbol_name, signal.side, entry_price))
      {
         record.decision_result = "rejected";
         record.rejection_reason = "ENTRY_PRICE_UNAVAILABLE";
         record.failed_execution_stage = "entry_price";
         return false;
      }

      double stop_loss = 0.0;
      long source_open_utc_ms = signal.absorption_candle_time_utc_ms > 0 ? signal.absorption_candle_time_utc_ms : signal.source_candle_open_time_utc_ms;
      if(signal.stop_reference_price <= 0.0)
      {
         record.entry_price = entry_price;
         record.source_candle_open_time_utc_ms = source_open_utc_ms;
         record.decision_result = "rejected";
         record.rejection_reason = "INVALID_STOP_REFERENCE_PRICE";
         record.failed_execution_stage = "stop_reference";
         return false;
      }
      stop_loss = NormalizeSymbolPrice(signal.symbol_name, signal.stop_reference_price);
      if(!StopLossBehindEntry(signal.side, entry_price, stop_loss))
      {
         record.entry_price = entry_price;
         record.source_candle_open_time_utc_ms = source_open_utc_ms;
         record.decision_result = "rejected";
         record.rejection_reason = "INVALID_STOP_REFERENCE_PRICE";
         record.failed_execution_stage = "stop_reference";
         return false;
      }

      int post_stop_record_index = FindPostStopRecordIndex(signal.symbol_name, signal.side);
      bool post_stop_stop_loss_used = (post_stop_record_index >= 0);
      if(post_stop_record_index >= 0)
      {
         record.stop_hit_price = m_post_stop_hit_prices[post_stop_record_index];
         record.post_stop_delay_seconds = m_post_stop_reentry_delay_seconds;
         record.reentry_stop_buffer = 0.0;
      }

      record.entry_price = entry_price;
      record.stop_loss = stop_loss;
      record.source_candle_open_time_utc_ms = source_open_utc_ms;

      double one_lot_loss = 0.0;
      if(!CalculateLossForVolume(signal.symbol_name, signal.side, 1.0, entry_price, stop_loss, one_lot_loss))
      {
         record.decision_result = "rejected";
         record.rejection_reason = "STOP_LOSS_DISTANCE_INVALID";
         record.failed_execution_stage = "stop_loss_distance";
         return false;
      }

      double risk_amount = AccountInfoDouble(ACCOUNT_BALANCE) * (m_risk_percent / 100.0);
      double requested_volume = NormalizeVolumeDown(signal.symbol_name, risk_amount / one_lot_loss);
      record.requested_volume = requested_volume;
      if(requested_volume <= 0.0)
      {
         record.decision_result = "rejected";
         record.rejection_reason = "REQUESTED_VOLUME_BELOW_MINIMUM";
         record.failed_execution_stage = "volume";
         return false;
      }

      double current_volume = requested_volume;
      int reduction_step = 0;
      double last_required_margin = 0.0;
      double last_free_before = 0.0;
      double last_free_after = 0.0;
      bool last_safety_passed = false;
      string last_rejection_reason = "VOLUME_REDUCED_BELOW_MINIMUM";
      string last_failed_stage = "volume";
      while(current_volume > 0.0)
      {
         double required_margin = 0.0;
         double free_before = 0.0;
         double free_after = 0.0;
         bool safety_passed = false;
         bool margin_passed = ValidateMarginAndSafety(
            signal,
            entry_price,
            stop_loss,
            current_volume,
            required_margin,
            free_before,
            free_after,
            safety_passed
         );

         double check_margin = 0.0;
         uint check_retcode = 0;
         string check_comment = "";
         bool broker_accepts = false;
         last_required_margin = required_margin;
         last_free_before = free_before;
         last_free_after = free_after;
         last_safety_passed = safety_passed;
         if(!margin_passed)
         {
            last_rejection_reason = "MARGIN_SAFETY_FAILED";
            last_failed_stage = "margin_safety";
         }
         else
         {
            broker_accepts = BrokerWouldAcceptOrder(
               signal,
               entry_price,
               stop_loss,
               current_volume,
               check_margin,
               check_retcode,
               check_comment
            );
            record.order_check_retcode = check_retcode;
            record.order_check_comment = check_comment;
            if(!broker_accepts)
            {
               last_rejection_reason = "ORDER_CHECK_FAILED";
               last_failed_stage = "order_check";
            }
         }

         if(broker_accepts)
         {
            uint send_retcode = 0;
            string send_comment = "";
            if(SendMarketOrder(signal, entry_price, stop_loss, current_volume, send_retcode, send_comment))
            {
               record.decision_result = "approved";
               record.final_volume = current_volume;
               record.required_margin = required_margin;
               record.free_margin_before = free_before;
               record.free_margin_after = free_after;
               record.margin_safety_passed = safety_passed;
               record.volume_reduction_step = reduction_step;
               record.order_send_retcode = send_retcode;
               record.order_send_comment = send_comment;
               string opened_position_id = "";
               string client_position_id = "";
               string client_position_identifier = "";
               string execution_comment = "";
               double opened_profit = 0.0;
               double opened_entry_price = 0.0;
               long opened_at_utc_ms = 0;
               if(FindOpenPositionDetails(
                  "",
                  signal.symbol_name,
                  signal.side,
                  opened_position_id,
                  client_position_id,
                  client_position_identifier,
                  execution_comment,
                  opened_profit,
                  opened_entry_price,
                  opened_at_utc_ms
               ))
               {
                  record.position_id = opened_position_id;
                  record.client_position_id = client_position_id;
                  record.client_position_identifier = client_position_identifier;
                  record.execution_comment = execution_comment;
                  record.entry_price = opened_entry_price;
               }
               if(post_stop_stop_loss_used)
                  RemovePostStopRecordAt(post_stop_record_index);
               return true;
            }
            record.order_send_retcode = send_retcode;
            record.order_send_comment = send_comment;
            last_rejection_reason = "ORDER_SEND_FAILED";
            last_failed_stage = "order_send";
         }

         current_volume = PreviousVolumeStep(signal.symbol_name, current_volume);
         reduction_step++;
      }

      record.decision_result = "rejected";
      record.rejection_reason = last_rejection_reason;
      record.failed_execution_stage = last_failed_stage;
      record.required_margin = last_required_margin;
      record.free_margin_before = last_free_before;
      record.free_margin_after = last_free_after;
      record.margin_safety_passed = last_safety_passed;
      record.volume_reduction_step = reduction_step;
      return false;
   }

   virtual bool ClosePosition(const string symbol, const ENUM_EXECUTION_SIDE side, const string timeframe, const string reason, SExecutionDecisionRecord &record)
   {
      InitializeDecisionRecord(record, ClientName(), symbol, timeframe, side, "exit", "");
      record.exit_trigger_reason = reason;
      for(int i = 0; i < PositionsTotal(); i++)
      {
         ulong ticket = PositionGetTicket(i);
         if(ticket == 0 || !PositionSelectByTicket(ticket))
            continue;
         if(PositionGetString(POSITION_SYMBOL) != symbol)
            continue;
         long position_type = PositionGetInteger(POSITION_TYPE);
         if(side == EXECUTION_SIDE_BUY && position_type != POSITION_TYPE_BUY)
            continue;
         if(side == EXECUTION_SIDE_SELL && position_type != POSITION_TYPE_SELL)
            continue;

         record.client_position_id = IntegerToString((long)ticket);
         record.client_position_identifier = IntegerToString(PositionGetInteger(POSITION_IDENTIFIER));
         record.position_id = CanonicalPositionId(record.client_position_id);
         record.execution_comment = PositionGetString(POSITION_COMMENT);
         if(PositionGetDouble(POSITION_PROFIT) <= 0.0)
         {
            record.decision_result = "rejected";
            record.rejection_reason = "CLOSE_SKIPPED_POSITION_NOT_PROFITABLE";
            return false;
         }
         CTrade trade;
         trade.SetDeviationInPoints(m_deviation_points);
         if(trade.PositionClose(ticket))
         {
            record.decision_result = "approved";
            return true;
         }
         record.decision_result = "rejected";
         record.rejection_reason = "POSITION_CLOSE_FAILED";
         return false;
      }
      record.decision_result = "rejected";
      record.rejection_reason = "OPEN_POSITION_NOT_FOUND";
      return false;
   }

   virtual bool ClosePositionCommand(const STradingExecutionSignal &signal, SExecutionDecisionRecord &record)
   {
      InitializeDecisionRecord(record, ClientName(), signal.symbol_name, signal.timeframe, signal.side, "exit", signal.position_id);
      record.request_id = signal.request_id;
      record.client_position_id = signal.client_position_id;
      record.client_position_identifier = signal.client_position_identifier;
      record.exit_trigger_reason = signal.exit_reason;
      bool has_position_identity = (signal.client_position_id != "" || signal.client_position_identifier != "");
      for(int i = 0; i < PositionsTotal(); i++)
      {
         ulong ticket = PositionGetTicket(i);
         if(ticket == 0 || !PositionSelectByTicket(ticket))
            continue;
         if(PositionGetString(POSITION_SYMBOL) != signal.symbol_name)
            continue;
         long position_type = PositionGetInteger(POSITION_TYPE);
         if(signal.side == EXECUTION_SIDE_BUY && position_type != POSITION_TYPE_BUY)
            continue;
         if(signal.side == EXECUTION_SIDE_SELL && position_type != POSITION_TYPE_SELL)
            continue;
         bool id_matches = PositionMatchesClientId(ticket, signal.client_position_id, signal.client_position_identifier);
         if(has_position_identity && !id_matches)
            continue;
         if(!has_position_identity && !IsOwnPositionFallbackMatch())
            continue;

         record.client_position_id = IntegerToString((long)ticket);
         record.client_position_identifier = IntegerToString(PositionGetInteger(POSITION_IDENTIFIER));
         record.position_id = CanonicalPositionId(record.client_position_id);
         record.execution_comment = PositionGetString(POSITION_COMMENT);
         if(PositionGetDouble(POSITION_PROFIT) <= 0.0)
         {
            record.decision_result = "rejected";
            record.rejection_reason = "CLOSE_SKIPPED_POSITION_NOT_PROFITABLE";
            return false;
         }
         CTrade trade;
         trade.SetDeviationInPoints(m_deviation_points);
         if(trade.PositionClose(ticket))
         {
            record.decision_result = "approved";
            return true;
         }
         record.decision_result = "rejected";
         record.rejection_reason = "POSITION_CLOSE_FAILED";
         return false;
      }
      record.decision_result = "rejected";
      record.rejection_reason = "OPEN_POSITION_NOT_FOUND";
      return false;
   }
};

#endif
