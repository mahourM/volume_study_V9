#property strict
#property version   "7.0"
#property description "VolumeStudy V7 startup shell"

#include <volume_study_V9/TcpBridgeClient.mqh>
#include <volume_study_V9/Execution/TradingStrategyEngine.mqh>
#include <volume_study_V9/Execution/TelegramAccountReporter.mqh>

<<<<<<< HEAD
=======
enum ENUM_TRADING_TIMEFRAME
{
   TRADING_TF_M1  = 0,
   TRADING_TF_M5  = 1,
   TRADING_TF_M15 = 2,
   TRADING_TF_M30 = 3,
   TRADING_TF_H1  = 4,
   TRADING_TF_H4  = 5,
   TRADING_TF_D1  = 6,
   TRADING_TF_W1  = 7
};

>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
input string InpServerHost = "127.0.0.1";
input int    InpServerPort = 5557;
input int    InpConnectTimeoutMs = 5000;
input int    InpCommandRecvTimeoutMs = 3000;
input string InpSymbolOverride = "";
<<<<<<< HEAD
=======
input ENUM_TRADING_TIMEFRAME InpTradingTimeframe = TRADING_TF_M5;
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
input double InpRiskPercent = 3.0;
input double InpInitialDepositUsd = 2000.0;
input double InpPreferredLeverage = 50.0;
input ulong  InpExecutionMagicNumber = 7009001;
input int    InpExecutionSignalPollMs = 1000;
input int    InpExecutionSignalRecvTimeoutMs = 25;
input int    InpExecutionStatusRecvTimeoutMs = 100;
input int    InpOrderDeviationPoints = 20;
input int    InpPostStopReentryDelaySeconds = 30;
input int    InpReentryStopBufferAtrPeriod = 14;
input double InpReentryStopBufferAtrMultiplier = 0.10;
input string InpExecutionDecisionCsvFile = "execution_decisions.csv";
input string InpTradingCommandTraceCsvFile = "trading_command_trace.csv";
input string InpTelegramConfigFileName = "telegram.local.csv";
input int    InpTelegramReportIntervalSeconds = 10800;
input int    InpChartLoadTimeoutMs = 10000;
input int    InpChartLoadStepTimeoutMs = 250;

CTcpBridgeClient gTcpBridgeClient;
CExecutionCsvLogger gExecutionCsvLogger;
CTradingCommandTraceCsvLogger gTradingCommandTraceCsvLogger;
CMetaTraderExecutionClient gMetaTraderExecutionClient;
CTradingStrategyEngine gTradingStrategyEngine;
CTelegramAccountReporter gTelegramAccountReporter;
bool   gDurationProfileRequestPending = false;
ulong  gDurationProfileRequestSentMs = 0;
ulong  gLastDurationProfilePollMs = 0;
string gPendingDurationProfileRequestId = "";
string gPendingDurationProfileSymbol = "";
string gPendingDurationProfileTimeframe = "";
bool   gLevelVolumeProfileRequestPending = false;
ulong  gLevelVolumeProfileRequestSentMs = 0;
ulong  gLastLevelVolumeProfilePollMs = 0;
string gPendingLevelVolumeProfileRequestId = "";
string gPendingLevelVolumeProfileSymbol = "";
string gPendingLevelVolumeProfileTimeframe = "";
bool   gVolumeZScoreProfileRequestPending = false;
ulong  gVolumeZScoreProfileRequestSentMs = 0;
ulong  gLastVolumeZScoreProfilePollMs = 0;
string gPendingVolumeZScoreProfileRequestId = "";
string gPendingVolumeZScoreProfileSymbol = "";
string gPendingVolumeZScoreProfileTimeframe = "";
bool   gTradingExecutionRequestPending = false;
ulong  gTradingExecutionRequestSentMs = 0;
ulong  gLastTradingExecutionPollMs = 0;
string gPendingTradingExecutionRequestId = "";
string gPendingTradingExecutionSymbol = "";
string gPendingTradingExecutionTimeframe = "";
<<<<<<< HEAD
string gPrimaryExecutionTimeframe = "";
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

string gDurationProfilePollTimeframes[] = {"M1", "M5", "M15", "M30", "H1", "H4"};
int gDurationProfilePollTimeframeIndex = 0;
int gLevelVolumeProfilePollTimeframeIndex = 0;
int gVolumeZScoreProfilePollTimeframeIndex = 0;

#define RESPONSE_INBOX_CAPACITY 16
#define DURATION_PROFILE_MAX_WIDTH_BARS 8
#define DURATION_PROFILE_RIGHT_OFFSET_BARS 1
#define LEVEL_VOLUME_PROFILE_MAX_WIDTH_BARS 4
#define LEVEL_VOLUME_PROFILE_GAP_BARS 1
#define VOLUME_ZSCORE_PROFILE_MAX_WIDTH_BARS 4
#define VOLUME_ZSCORE_PROFILE_GAP_BARS 1
#define CHART_SHIFT_SIZE_PERCENT 50.0

string gResponseInbox[RESPONSE_INBOX_CAPACITY];
int gResponseInboxCount = 0;
string gDurationProfileObjectNames[];
string gDurationProfileObjectKeys[];
long gDurationProfileObjectChartIds[];
string gLevelVolumeProfileObjectNames[];
string gLevelVolumeProfileObjectKeys[];
long gLevelVolumeProfileObjectChartIds[];
string gVolumeZScoreProfileObjectNames[];
string gVolumeZScoreProfileObjectKeys[];
long gVolumeZScoreProfileObjectChartIds[];
STradingExecutionSignal gLatestTradingExecutionSignals[];
string gTrackedPositionRequestIds[];
string gTrackedPositionIds[];
string gTrackedPositionClientIds[];
string gTrackedPositionSymbols[];
string gTrackedPositionTimeframes[];
int gTrackedPositionSides[];
long gTrackedPositionIdentifiers[];

struct SDurationProfile
{
   string symbol;
   string timeframe;
   long candle_open_time_utc_ms;
   long candle_close_time_utc_ms;
   long candle_duration_ms;
   double price_step;
   long max_duration_ms;
};

struct SDurationProfileLevel
{
   double price;
   long duration_ms;
   double duration_fraction;
   bool significant;
};

struct SLevelVolumeProfile
{
   string symbol;
   string timeframe;
   long candle_open_time_utc_ms;
   long candle_close_time_utc_ms;
   double price_step;
   double max_total_volume;
   long levels_count;
};

struct SLevelVolumeProfileLevel
{
   double price;
   double agg_buy_volume;
   double agg_sell_volume;
   double total_volume;
   double delta_volume;
   double volume_fraction;
};

struct SVolumeZScoreProfile
{
   string symbol;
   string timeframe;
   long candle_open_time_utc_ms;
   long candle_close_time_utc_ms;
   double fixed_bin_size;
   long baseline_count;
   double z_cap;
   double max_positive_volume_z_score;
   long bins_count;
};

struct SVolumeZScoreProfileBin
{
   double bin_low;
   double bin_high;
   double current_volume;
   double current_buy_volume;
   double current_sell_volume;
   double current_delta_volume;
   long baseline_count;
   double baseline_median_volume;
   double baseline_mad_volume;
   double effective_mad_volume;
   double volume_z_score;
   double positive_volume_z_score;
   double z_cap;
   double line_width_ratio;
};

string BuildRuntimeSymbol()
{
   if(InpSymbolOverride != "")
      return InpSymbolOverride;
   return _Symbol;
}

<<<<<<< HEAD
=======
string TradingTimeframeToText(const ENUM_TRADING_TIMEFRAME tf)
{
   if(tf == TRADING_TF_M1)  return "M1";
   if(tf == TRADING_TF_M5)  return "M5";
   if(tf == TRADING_TF_M15) return "M15";
   if(tf == TRADING_TF_M30) return "M30";
   if(tf == TRADING_TF_H1)  return "H1";
   if(tf == TRADING_TF_H4)  return "H4";
   if(tf == TRADING_TF_D1)  return "D1";
   if(tf == TRADING_TF_W1)  return "W1";
   return "H1";
}

>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
string EscapeJson(const string source_text)
{
   string escaped_text = source_text;
   StringReplace(escaped_text, "\\", "\\\\");
   StringReplace(escaped_text, "\"", "\\\"");
   StringReplace(escaped_text, "\r", " ");
   StringReplace(escaped_text, "\n", " ");
   return escaped_text;
}

bool ExtractJsonString(const string json_text, const string key, string &value_out)
{
   value_out = "";
   string pattern = "\"" + key + "\":\"";
   int start_pos = StringFind(json_text, pattern);
   if(start_pos < 0)
      return false;

   int value_pos = start_pos + StringLen(pattern);
   bool escaped = false;
   for(int i = value_pos; i < StringLen(json_text); i++)
   {
      ushort ch = StringGetCharacter(json_text, i);
      if(escaped)
      {
         value_out += ShortToString(ch);
         escaped = false;
         continue;
      }
      if(ch == 92)
      {
         escaped = true;
         continue;
      }
      if(ch == 34)
         return true;
      value_out += ShortToString(ch);
   }
   return false;
}

bool ExtractTopLevelJsonString(const string json_text, const string key, string &value_out)
{
   value_out = "";
   int length = StringLen(json_text);
   int depth = 0;
   bool in_string = false;
   bool escaped = false;

   for(int i = 0; i < length; i++)
   {
      ushort ch = StringGetCharacter(json_text, i);
      if(in_string)
      {
         if(escaped)
         {
            escaped = false;
            continue;
         }
         if(ch == 92)
         {
            escaped = true;
            continue;
         }
         if(ch == 34)
            in_string = false;
         continue;
      }

      if(ch == 34)
      {
         if(depth != 1)
         {
            in_string = true;
            continue;
         }

         string current_key = "";
         bool key_escaped = false;
         int key_end = -1;
         for(int j = i + 1; j < length; j++)
         {
            ushort key_ch = StringGetCharacter(json_text, j);
            if(key_escaped)
            {
               current_key += ShortToString(key_ch);
               key_escaped = false;
               continue;
            }
            if(key_ch == 92)
            {
               key_escaped = true;
               continue;
            }
            if(key_ch == 34)
            {
               key_end = j;
               break;
            }
            current_key += ShortToString(key_ch);
         }
         if(key_end < 0)
            return false;

         int colon_pos = key_end + 1;
         while(colon_pos < length)
         {
            ushort colon_ch = StringGetCharacter(json_text, colon_pos);
            if(colon_ch != 32 && colon_ch != 9 && colon_ch != 13 && colon_ch != 10)
               break;
            colon_pos++;
         }
         if(colon_pos >= length || StringGetCharacter(json_text, colon_pos) != 58)
         {
            i = key_end;
            continue;
         }

         int value_pos = colon_pos + 1;
         while(value_pos < length)
         {
            ushort value_ch = StringGetCharacter(json_text, value_pos);
            if(value_ch != 32 && value_ch != 9 && value_ch != 13 && value_ch != 10)
               break;
            value_pos++;
         }

         if(current_key == key)
         {
            if(value_pos >= length || StringGetCharacter(json_text, value_pos) != 34)
               return false;
            bool value_escaped = false;
            for(int k = value_pos + 1; k < length; k++)
            {
               ushort value_ch = StringGetCharacter(json_text, k);
               if(value_escaped)
               {
                  value_out += ShortToString(value_ch);
                  value_escaped = false;
                  continue;
               }
               if(value_ch == 92)
               {
                  value_escaped = true;
                  continue;
               }
               if(value_ch == 34)
                  return true;
               value_out += ShortToString(value_ch);
            }
            return false;
         }

         i = key_end;
         continue;
      }

      if(ch == 123 || ch == 91)
         depth++;
      else if(ch == 125 || ch == 93)
         depth--;
   }
   return false;
}

bool ExtractJsonRawValue(const string json_text, const string key, string &value_out)
{
   value_out = "";
   string pattern = "\"" + key + "\":";
   int start_pos = StringFind(json_text, pattern);
   if(start_pos < 0)
      return false;

   int value_pos = start_pos + StringLen(pattern);
   while(value_pos < StringLen(json_text))
   {
      ushort ch = StringGetCharacter(json_text, value_pos);
      if(ch != 32 && ch != 9 && ch != 13 && ch != 10)
         break;
      value_pos++;
   }

   for(int i = value_pos; i < StringLen(json_text); i++)
   {
      ushort ch = StringGetCharacter(json_text, i);
      if(ch == 44 || ch == 125)
         break;
      value_out += ShortToString(ch);
   }
   return (value_out != "");
}

bool ExtractJsonArray(const string json_text, const string key, string &value_out)
{
   value_out = "";
   string pattern = "\"" + key + "\":[";
   int start_pos = StringFind(json_text, pattern);
   if(start_pos < 0)
      return false;

   int array_start = start_pos + StringLen(pattern) - 1;
   int depth = 0;
   bool in_string = false;
   bool escaped = false;
   for(int i = array_start; i < StringLen(json_text); i++)
   {
      ushort ch = StringGetCharacter(json_text, i);
      if(in_string)
      {
         if(escaped)
         {
            escaped = false;
            continue;
         }
         if(ch == 92)
         {
            escaped = true;
            continue;
         }
         if(ch == 34)
            in_string = false;
         continue;
      }
      if(ch == 34)
      {
         in_string = true;
         continue;
      }
      if(ch == 91)
         depth++;
      else if(ch == 93)
      {
         depth--;
         if(depth == 0)
         {
            value_out = StringSubstr(json_text, array_start, i - array_start + 1);
            return true;
         }
      }
   }
   return false;
}

bool ExtractNextJsonObject(const string array_text, const int start_pos, string &object_text_out, int &next_pos_out)
{
   object_text_out = "";
   next_pos_out = start_pos;
   int object_start = -1;
   for(int i = start_pos; i < StringLen(array_text); i++)
   {
      if(StringGetCharacter(array_text, i) == 123)
      {
         object_start = i;
         break;
      }
   }
   if(object_start < 0)
      return false;

   int depth = 0;
   bool in_string = false;
   bool escaped = false;
   for(int i = object_start; i < StringLen(array_text); i++)
   {
      ushort ch = StringGetCharacter(array_text, i);
      if(in_string)
      {
         if(escaped)
         {
            escaped = false;
            continue;
         }
         if(ch == 92)
         {
            escaped = true;
            continue;
         }
         if(ch == 34)
            in_string = false;
         continue;
      }
      if(ch == 34)
      {
         in_string = true;
         continue;
      }
      if(ch == 123)
         depth++;
      else if(ch == 125)
      {
         depth--;
         if(depth == 0)
         {
            object_text_out = StringSubstr(array_text, object_start, i - object_start + 1);
            next_pos_out = i + 1;
            return true;
         }
      }
   }
   return false;
}

bool ExtractJsonDouble(const string json_text, const string key, double &value_out)
{
   string raw_value = "";
   if(!ExtractJsonRawValue(json_text, key, raw_value))
      return false;
   value_out = StringToDouble(raw_value);
   return true;
}
bool ExtractJsonBool(
   const string json_text,
   const string key,
   bool &value
)
{
   string pattern = "\"" + key + "\":";

   int start_pos = StringFind(json_text, pattern);

   if(start_pos < 0)
      return false;

   start_pos += StringLen(pattern);

   while(start_pos < StringLen(json_text))
   {
      ushort ch = StringGetCharacter(json_text, start_pos);

      if(ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n')
         break;

      start_pos++;
   }

   string remaining = StringSubstr(json_text, start_pos);

   if(StringFind(remaining, "true") == 0)
   {
      value = true;
      return true;
   }

   if(StringFind(remaining, "false") == 0)
   {
      value = false;
      return true;
   }

   return false;
}
bool ExtractJsonLong(const string json_text, const string key, long &value_out)
{
   string raw_value = "";
   if(!ExtractJsonRawValue(json_text, key, raw_value))
      return false;
   value_out = (long)StringToInteger(raw_value);
   return true;
}

string BuildRequestId(const string request_type, const string symbol, const string timeframe)
{
   return StringFormat(
      "%s:%s:%s:%I64d:%I64d",
      request_type,
      symbol,
      timeframe,
      (long)TimeCurrent(),
      (long)GetTickCount64()
   );
}

bool ExtractMessageType(const string message_text, string &message_type_out)
{
   message_type_out = "";
   return ExtractJsonString(message_text, "type", message_type_out);
}

bool IsDurationProfileResponseType(const string message_type)
{
   return (
      message_type == "DURATION_PROFILE" ||
      message_type == "NO_DURATION_PROFILE"
   );
}

bool IsLevelVolumeProfileResponseType(const string message_type)
{
   return (
      message_type == "LEVEL_VOLUME_PROFILE" ||
      message_type == "NO_LEVEL_VOLUME_PROFILE"
   );
}

bool IsVolumeZScoreProfileResponseType(const string message_type)
{
   return (
      message_type == "VOLUME_ZSCORE_PROFILE" ||
      message_type == "NO_VOLUME_ZSCORE_PROFILE"
   );
}

bool IsTradingExecutionResponseType(const string message_type)
{
   return (
      message_type == "TRADING_EXECUTION_SIGNAL_LIST" ||
      message_type == "NO_TRADING_EXECUTION_SIGNAL" ||
      message_type == "TRADING_EXECUTION_COMMAND_LIST" ||
      message_type == "NO_TRADING_EXECUTION_COMMAND"
   );
}

bool IsTradingPositionStatusAckType(const string message_type)
{
   return (message_type == "TRADING_POSITION_STATUS_ACK");
}

bool IsErrorResponseType(const string message_type)
{
   return (message_type == "ERROR");
}

bool IsSupportedMt5AbsorptionTimeframe(const string timeframe)
{
   return (
      timeframe == "M1" ||
      timeframe == "M2" ||
      timeframe == "M4" ||
      timeframe == "M5" ||
      timeframe == "M10" ||
      timeframe == "M15" ||
      timeframe == "M30" ||
      timeframe == "H1" ||
      timeframe == "H4"
   );
}

bool IsSupportedTradeSide(const string side)
{
   return (side == "BUY" || side == "SELL");
}

bool MessageMatchesPendingDurationProfileRequest(const string message_text)
{
   if(!gDurationProfileRequestPending || gPendingDurationProfileRequestId == "")
      return false;

   string message_type = "";
   if(!ExtractMessageType(message_text, message_type))
      return false;
   if(!IsDurationProfileResponseType(message_type) && !IsErrorResponseType(message_type))
      return false;

   string request_id = "";
   if(!ExtractJsonString(message_text, "request_id", request_id))
      return false;
   if(request_id != gPendingDurationProfileRequestId)
      return false;

   if(IsErrorResponseType(message_type))
      return true;

   string response_symbol = "";
   if(!ExtractJsonString(message_text, "symbol", response_symbol))
      return false;
   if(response_symbol != gPendingDurationProfileSymbol)
      return false;

   string response_timeframe = "";
   if(ExtractJsonString(message_text, "timeframe", response_timeframe) && response_timeframe != gPendingDurationProfileTimeframe)
      return false;

   return true;
}

bool MessageMatchesPendingLevelVolumeProfileRequest(const string message_text)
{
   if(!gLevelVolumeProfileRequestPending || gPendingLevelVolumeProfileRequestId == "")
      return false;

   string message_type = "";
   if(!ExtractMessageType(message_text, message_type))
      return false;
   if(!IsLevelVolumeProfileResponseType(message_type) && !IsErrorResponseType(message_type))
      return false;

   string request_id = "";
   if(!ExtractJsonString(message_text, "request_id", request_id))
      return false;
   if(request_id != gPendingLevelVolumeProfileRequestId)
      return false;

   if(IsErrorResponseType(message_type))
      return true;

   string response_symbol = "";
   if(!ExtractJsonString(message_text, "symbol", response_symbol))
      return false;
   if(response_symbol != gPendingLevelVolumeProfileSymbol)
      return false;

   string response_timeframe = "";
   if(ExtractJsonString(message_text, "timeframe", response_timeframe) && response_timeframe != gPendingLevelVolumeProfileTimeframe)
      return false;

   return true;
}

bool MessageMatchesPendingVolumeZScoreProfileRequest(const string message_text)
{
   if(!gVolumeZScoreProfileRequestPending || gPendingVolumeZScoreProfileRequestId == "")
      return false;

   string message_type = "";
   if(!ExtractMessageType(message_text, message_type))
      return false;
   if(!IsVolumeZScoreProfileResponseType(message_type) && !IsErrorResponseType(message_type))
      return false;

   string request_id = "";
   if(!ExtractJsonString(message_text, "request_id", request_id))
      return false;
   if(request_id != gPendingVolumeZScoreProfileRequestId)
      return false;

   if(IsErrorResponseType(message_type))
      return true;

   string response_symbol = "";
   if(!ExtractJsonString(message_text, "symbol", response_symbol))
      return false;
   if(response_symbol != gPendingVolumeZScoreProfileSymbol)
      return false;

   string response_timeframe = "";
   if(ExtractJsonString(message_text, "timeframe", response_timeframe) && response_timeframe != gPendingVolumeZScoreProfileTimeframe)
      return false;

   return true;
}

bool MessageMatchesPendingTradingExecutionRequest(const string message_text)
{
   if(!gTradingExecutionRequestPending || gPendingTradingExecutionRequestId == "")
      return false;

   string message_type = "";
   if(!ExtractTopLevelJsonString(message_text, "type", message_type))
      return false;
   if(!IsTradingExecutionResponseType(message_type) && !IsErrorResponseType(message_type))
      return false;

   string request_id = "";
   if(!ExtractTopLevelJsonString(message_text, "request_id", request_id))
      return false;
   if(request_id != gPendingTradingExecutionRequestId)
      return false;

   if(IsErrorResponseType(message_type))
      return true;

   if(gPendingTradingExecutionSymbol == "")
      return true;

   string response_symbol = "";
   if(ExtractTopLevelJsonString(message_text, "symbol", response_symbol) && response_symbol != gPendingTradingExecutionSymbol)
      return false;

   string response_primary_timeframe = "";
   if(ExtractTopLevelJsonString(message_text, "primary_timeframe", response_primary_timeframe) && response_primary_timeframe != gPendingTradingExecutionTimeframe)
      return false;

   return true;
}

bool DiagnoseTradingExecutionResponseMatch(
   const string message_text,
   string &message_type,
   string &request_id,
   string &response_symbol,
   string &response_primary_timeframe,
   string &reason
)
{
   message_type = "";
   request_id = "";
   response_symbol = "";
   response_primary_timeframe = "";
   reason = "";

   if(!gTradingExecutionRequestPending || gPendingTradingExecutionRequestId == "")
   {
      reason = "NO_PENDING_REQUEST";
      return false;
   }
   if(!ExtractTopLevelJsonString(message_text, "type", message_type))
   {
      reason = "MISSING_MESSAGE_TYPE";
      return false;
   }
   if(!IsTradingExecutionResponseType(message_type) && !IsErrorResponseType(message_type))
   {
      reason = "NON_TRADING_RESPONSE";
      return false;
   }
   if(!ExtractTopLevelJsonString(message_text, "request_id", request_id))
   {
      reason = "MISSING_REQUEST_ID";
      return false;
   }
   if(request_id != gPendingTradingExecutionRequestId)
   {
      reason = "REQUEST_ID_MISMATCH";
      return false;
   }
   if(IsErrorResponseType(message_type))
      return true;

   ExtractTopLevelJsonString(message_text, "symbol", response_symbol);
   if(response_symbol != "" && response_symbol != gPendingTradingExecutionSymbol)
   {
      reason = "SYMBOL_MISMATCH";
      return false;
   }

   ExtractTopLevelJsonString(message_text, "primary_timeframe", response_primary_timeframe);
   if(response_primary_timeframe != "" && response_primary_timeframe != gPendingTradingExecutionTimeframe)
   {
      reason = "PRIMARY_TIMEFRAME_MISMATCH";
      return false;
   }

   return true;
}

void RemoveInboxMessageAt(const int index)
{
   if(index < 0 || index >= gResponseInboxCount)
      return;
   for(int i = index; i < gResponseInboxCount - 1; i++)
      gResponseInbox[i] = gResponseInbox[i + 1];
   gResponseInbox[gResponseInboxCount - 1] = "";
   gResponseInboxCount--;
}

void PurgeStaleInboxMessages()
{
   for(int i = gResponseInboxCount - 1; i >= 0; i--)
   {
      if(
         !MessageMatchesPendingDurationProfileRequest(gResponseInbox[i]) &&
         !MessageMatchesPendingLevelVolumeProfileRequest(gResponseInbox[i]) &&
         !MessageMatchesPendingVolumeZScoreProfileRequest(gResponseInbox[i]) &&
         !MessageMatchesPendingTradingExecutionRequest(gResponseInbox[i])
      )
         RemoveInboxMessageAt(i);
   }
}

bool StoreInboxMessage(const string message_text)
{
   if(message_text == "")
      return false;
   PurgeStaleInboxMessages();
   for(int i = 0; i < gResponseInboxCount; i++)
   {
      if(gResponseInbox[i] == message_text)
         return true;
   }
   if(gResponseInboxCount >= RESPONSE_INBOX_CAPACITY)
   {
      RemoveInboxMessageAt(0);
   }
   gResponseInbox[gResponseInboxCount] = message_text;
   gResponseInboxCount++;
   return true;
}

bool StoreInboxMessageIfPending(const string message_text)
{
   if(
      MessageMatchesPendingDurationProfileRequest(message_text) ||
      MessageMatchesPendingLevelVolumeProfileRequest(message_text) ||
      MessageMatchesPendingVolumeZScoreProfileRequest(message_text) ||
      MessageMatchesPendingTradingExecutionRequest(message_text)
   )
      return StoreInboxMessage(message_text);
   return false;
}

bool TakeInboxDurationProfileResponse(string &message_text_out)
{
   message_text_out = "";
   for(int i = 0; i < gResponseInboxCount; i++)
   {
      if(MessageMatchesPendingDurationProfileRequest(gResponseInbox[i]))
      {
         message_text_out = gResponseInbox[i];
         RemoveInboxMessageAt(i);
         return true;
      }
   }
   return false;
}

bool TakeInboxLevelVolumeProfileResponse(string &message_text_out)
{
   message_text_out = "";
   for(int i = 0; i < gResponseInboxCount; i++)
   {
      if(MessageMatchesPendingLevelVolumeProfileRequest(gResponseInbox[i]))
      {
         message_text_out = gResponseInbox[i];
         RemoveInboxMessageAt(i);
         return true;
      }
   }
   return false;
}

bool TakeInboxVolumeZScoreProfileResponse(string &message_text_out)
{
   message_text_out = "";
   for(int i = 0; i < gResponseInboxCount; i++)
   {
      if(MessageMatchesPendingVolumeZScoreProfileRequest(gResponseInbox[i]))
      {
         message_text_out = gResponseInbox[i];
         RemoveInboxMessageAt(i);
         return true;
      }
   }
   return false;
}

bool TakeInboxTradingExecutionResponse(string &message_text_out)
{
   message_text_out = "";
   for(int i = 0; i < gResponseInboxCount; i++)
   {
      if(MessageMatchesPendingTradingExecutionRequest(gResponseInbox[i]))
      {
         message_text_out = gResponseInbox[i];
         RemoveInboxMessageAt(i);
         return true;
      }
   }
   return false;
}

void ClearPendingDurationProfileRequest()
{
   gDurationProfileRequestPending = false;
   gPendingDurationProfileRequestId = "";
   gPendingDurationProfileSymbol = "";
   gPendingDurationProfileTimeframe = "";
   PurgeStaleInboxMessages();
}

void ClearPendingLevelVolumeProfileRequest()
{
   gLevelVolumeProfileRequestPending = false;
   gPendingLevelVolumeProfileRequestId = "";
   gPendingLevelVolumeProfileSymbol = "";
   gPendingLevelVolumeProfileTimeframe = "";
   PurgeStaleInboxMessages();
}

void ClearPendingVolumeZScoreProfileRequest()
{
   gVolumeZScoreProfileRequestPending = false;
   gPendingVolumeZScoreProfileRequestId = "";
   gPendingVolumeZScoreProfileSymbol = "";
   gPendingVolumeZScoreProfileTimeframe = "";
   PurgeStaleInboxMessages();
}

void ClearPendingTradingExecutionRequest()
{
   gTradingExecutionRequestPending = false;
   gPendingTradingExecutionRequestId = "";
   gPendingTradingExecutionSymbol = "";
   gPendingTradingExecutionTimeframe = "";
   PurgeStaleInboxMessages();
}

void RecordTradingCommandParseTrace(
   const string stage,
   const string request_id,
   const string message_type,
   const string response_symbol,
   const string response_primary_timeframe,
   const int command_index,
   const string command_type,
   const string symbol_name,
   const string timeframe,
   const string side,
   const string position_id,
   const string result,
<<<<<<< HEAD
   const string reason,
   const string action = "",
   const long target_entry_open_time_utc_ms = 0
=======
   const string reason
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
)
{
   gTradingCommandTraceCsvLogger.Record(
      stage,
      request_id,
      gPendingTradingExecutionRequestId,
      message_type,
      response_symbol,
      gPendingTradingExecutionSymbol,
      response_primary_timeframe,
      gPendingTradingExecutionTimeframe,
      command_index,
      command_type,
      symbol_name,
      "",
      timeframe,
      side,
      position_id,
      0,
      0,
      result,
<<<<<<< HEAD
      reason,
      action,
      target_entry_open_time_utc_ms
=======
      reason
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   );
}

bool ParseTradingExecutionSignalFields(
   const string json_text,
   STradingExecutionSignal &signal,
   const int command_index,
   const string request_id,
   const string message_type,
   const string response_symbol,
   const string response_primary_timeframe
)
{
   string side_text = "";
   signal.command_type = "";
<<<<<<< HEAD
   signal.action = "";
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   signal.request_id = "";
   signal.position_id = "";
   signal.symbol_name = "";
   signal.timeframe = "";
   signal.side = EXECUTION_SIDE_NONE;
   signal.signal_time_utc_ms = 0;
<<<<<<< HEAD
   signal.target_entry_open_time_utc_ms = 0;
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   signal.cluster_id = "";
   signal.client_name = "";
   signal.client_position_id = "";
   signal.client_position_identifier = "";
   signal.source_candle_open_time_utc_ms = 0;
   signal.source_candle_close_time_utc_ms = 0;
   signal.zone_low = 0.0;
   signal.zone_high = 0.0;
   signal.stop_reference_price = 0.0;
   signal.absorption_candle_time_utc_ms = 0;
   signal.dominance_candle_time_utc_ms = 0;
   signal.trigger_bin_price = 0.0;
   signal.entry_reason = "";
   signal.exit_reason = "";

   if(!ExtractJsonString(json_text, "command_type", signal.command_type))
      signal.command_type = "OPEN";
   StringToUpper(signal.command_type);
<<<<<<< HEAD
   if(!ExtractJsonString(json_text, "action", signal.action))
      signal.action = "";
   StringToUpper(signal.action);
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   if(!ExtractJsonString(json_text, "request_id", signal.request_id))
      signal.request_id = "";
   if(!ExtractJsonString(json_text, "position_id", signal.position_id))
      signal.position_id = "";
   if(!ExtractJsonString(json_text, "symbol_name", signal.symbol_name))
   {
      RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, "", "", "", signal.position_id, "rejected", "MISSING_SYMBOL_NAME");
      return false;
   }
   if(!ExtractJsonString(json_text, "timeframe", signal.timeframe))
   {
      RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, "", "", signal.position_id, "rejected", "MISSING_TIMEFRAME");
      return false;
   }
   if(!ExtractJsonString(json_text, "side", side_text))
   {
      RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, "", signal.position_id, "rejected", "MISSING_SIDE");
      return false;
   }
   signal.side = ExecutionSideFromText(side_text);
   if(signal.side == EXECUTION_SIDE_NONE)
   {
      RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, side_text, signal.position_id, "rejected", "INVALID_SIDE");
      return false;
   }
   if(!ExtractJsonLong(json_text, "signal_time", signal.signal_time_utc_ms))
      signal.signal_time_utc_ms = 0;
<<<<<<< HEAD
   if(!ExtractJsonLong(json_text, "target_entry_open_time_utc_ms", signal.target_entry_open_time_utc_ms))
      signal.target_entry_open_time_utc_ms = 0;
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   if(!ExtractJsonString(json_text, "cluster_id", signal.cluster_id))
      signal.cluster_id = signal.request_id != "" ? signal.request_id : signal.position_id;
   if(!ExtractJsonString(json_text, "client_name", signal.client_name))
      signal.client_name = "metatrader";
   if(!ExtractJsonString(json_text, "client_position_id", signal.client_position_id))
      signal.client_position_id = "";
   if(!ExtractJsonString(json_text, "client_position_identifier", signal.client_position_identifier))
      signal.client_position_identifier = "";
   if(!ExtractJsonLong(json_text, "source_candle_open_time_utc_ms", signal.source_candle_open_time_utc_ms))
      signal.source_candle_open_time_utc_ms = 0;
   if(!ExtractJsonLong(json_text, "source_candle_close_time_utc_ms", signal.source_candle_close_time_utc_ms))
      signal.source_candle_close_time_utc_ms = 0;
   if(!ExtractJsonDouble(json_text, "zone_low", signal.zone_low))
      signal.zone_low = 0.0;
   if(!ExtractJsonDouble(json_text, "zone_high", signal.zone_high))
      signal.zone_high = 0.0;
   if(!ExtractJsonDouble(json_text, "stop_reference_price", signal.stop_reference_price))
      signal.stop_reference_price = 0.0;
   if(!ExtractJsonLong(json_text, "absorption_candle_time_utc_ms", signal.absorption_candle_time_utc_ms))
      signal.absorption_candle_time_utc_ms = 0;
   if(!ExtractJsonLong(json_text, "dominance_candle_time_utc_ms", signal.dominance_candle_time_utc_ms))
      signal.dominance_candle_time_utc_ms = 0;
   if(!ExtractJsonDouble(json_text, "trigger_bin_price", signal.trigger_bin_price))
      signal.trigger_bin_price = 0.0;
   if(!ExtractJsonString(json_text, "entry_reason", signal.entry_reason))
      signal.entry_reason = "";
   if(!ExtractJsonString(json_text, "exit_reason", signal.exit_reason))
      signal.exit_reason = "";
   if(signal.source_candle_open_time_utc_ms <= 0 && signal.absorption_candle_time_utc_ms > 0)
      signal.source_candle_open_time_utc_ms = signal.absorption_candle_time_utc_ms;
   if(signal.source_candle_close_time_utc_ms <= 0 && signal.dominance_candle_time_utc_ms > 0)
      signal.source_candle_close_time_utc_ms = signal.dominance_candle_time_utc_ms;
   if(signal.command_type != "OPEN" && signal.command_type != "CLOSE")
   {
      RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, ExecutionSideToText(signal.side), signal.position_id, "rejected", "INVALID_COMMAND_TYPE");
      return false;
   }
<<<<<<< HEAD
   if(signal.action == "")
   {
      if(signal.command_type == "OPEN" && signal.side == EXECUTION_SIDE_BUY)
         signal.action = "ENTRY_BUY";
      else if(signal.command_type == "OPEN" && signal.side == EXECUTION_SIDE_SELL)
         signal.action = "ENTRY_SELL";
      else if(signal.command_type == "CLOSE" && signal.side == EXECUTION_SIDE_BUY)
         signal.action = "EXIT_BUY_POSITION";
      else if(signal.command_type == "CLOSE" && signal.side == EXECUTION_SIDE_SELL)
         signal.action = "EXIT_SELL_POSITION";
   }
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   if(signal.command_type == "OPEN" && signal.request_id == "" && signal.position_id == "")
   {
      RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, ExecutionSideToText(signal.side), signal.position_id, "rejected", "MISSING_REQUEST_ID");
      return false;
   }
   if(signal.command_type == "CLOSE" && signal.client_position_id == "" && signal.client_position_identifier == "" && signal.position_id == "")
   {
      RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, ExecutionSideToText(signal.side), signal.position_id, "rejected", "MISSING_POSITION_ID");
      return false;
   }
   if(signal.command_type == "OPEN")
   {
      if(signal.source_candle_close_time_utc_ms < signal.source_candle_open_time_utc_ms)
      {
         RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, ExecutionSideToText(signal.side), signal.position_id, "rejected", "INVALID_CANDLE_TIME");
         return false;
      }
      if(signal.stop_reference_price <= 0.0)
      {
         RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, ExecutionSideToText(signal.side), signal.position_id, "rejected", "INVALID_STOP_REFERENCE_PRICE");
         return false;
      }
      if(signal.dominance_candle_time_utc_ms > 0 && signal.absorption_candle_time_utc_ms > 0 && signal.dominance_candle_time_utc_ms < signal.absorption_candle_time_utc_ms)
      {
         RecordTradingCommandParseTrace("COMMAND_PARSE_REJECT", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, ExecutionSideToText(signal.side), signal.position_id, "rejected", "INVALID_ENTRY_STATE_TIME");
         return false;
      }
   }
<<<<<<< HEAD
   RecordTradingCommandParseTrace("COMMAND_PARSED", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, ExecutionSideToText(signal.side), signal.position_id, "parsed", "", signal.action, signal.target_entry_open_time_utc_ms);
=======
   RecordTradingCommandParseTrace("COMMAND_PARSED", request_id, message_type, response_symbol, response_primary_timeframe, command_index, signal.command_type, signal.symbol_name, signal.timeframe, ExecutionSideToText(signal.side), signal.position_id, "parsed", "");
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   return true;
}

bool ParseTradingExecutionSignalListPayload(const string json_text, STradingExecutionSignal &signals_out[])
{
   ArrayResize(signals_out, 0);
   string message_type = "";
   string request_id = "";
   string response_symbol = "";
   string response_primary_timeframe = "";
   ExtractTopLevelJsonString(json_text, "request_id", request_id);
   ExtractTopLevelJsonString(json_text, "symbol", response_symbol);
   ExtractTopLevelJsonString(json_text, "primary_timeframe", response_primary_timeframe);

   if(!ExtractTopLevelJsonString(json_text, "type", message_type))
   {
      gTradingCommandTraceCsvLogger.Record("COMMAND_LIST_PARSE_RESULT", request_id, gPendingTradingExecutionRequestId, "", response_symbol, gPendingTradingExecutionSymbol, response_primary_timeframe, gPendingTradingExecutionTimeframe, -1, "", "", "", "", "", "", 0, 0, "rejected", "MISSING_MESSAGE_TYPE");
      return false;
   }
   gTradingCommandTraceCsvLogger.Record("COMMAND_LIST_PARSE_START", request_id, gPendingTradingExecutionRequestId, message_type, response_symbol, gPendingTradingExecutionSymbol, response_primary_timeframe, gPendingTradingExecutionTimeframe, -1, "", "", "", "", "", "", 0, 0, "start", "");
   if(message_type == "NO_TRADING_EXECUTION_SIGNAL" || message_type == "NO_TRADING_EXECUTION_COMMAND")
   {
      gTradingCommandTraceCsvLogger.Record("NO_COMMAND_RESPONSE", request_id, gPendingTradingExecutionRequestId, message_type, response_symbol, gPendingTradingExecutionSymbol, response_primary_timeframe, gPendingTradingExecutionTimeframe, -1, "", "", "", "", "", "", 0, 0, "ok", "");
      return true;
   }
   if(message_type != "TRADING_EXECUTION_SIGNAL_LIST" && message_type != "TRADING_EXECUTION_COMMAND_LIST")
   {
      gTradingCommandTraceCsvLogger.Record("COMMAND_LIST_PARSE_RESULT", request_id, gPendingTradingExecutionRequestId, message_type, response_symbol, gPendingTradingExecutionSymbol, response_primary_timeframe, gPendingTradingExecutionTimeframe, -1, "", "", "", "", "", "", 0, 0, "rejected", "UNSUPPORTED_MESSAGE_TYPE");
      return false;
   }

   string signals_text = "";
   if(!ExtractJsonArray(json_text, "commands", signals_text))
   {
      if(!ExtractJsonArray(json_text, "signals", signals_text))
      {
         gTradingCommandTraceCsvLogger.Record("COMMAND_LIST_PARSE_RESULT", request_id, gPendingTradingExecutionRequestId, message_type, response_symbol, gPendingTradingExecutionSymbol, response_primary_timeframe, gPendingTradingExecutionTimeframe, -1, "", "", "", "", "", "", 0, 0, "rejected", "MISSING_COMMANDS_ARRAY");
         return false;
      }
   }

   int position = 0;
   int command_count = 0;
   int parsed_count = 0;
   while(position < StringLen(signals_text))
   {
      string signal_text = "";
      int next_position = position;
      if(!ExtractNextJsonObject(signals_text, position, signal_text, next_position))
         break;

      STradingExecutionSignal signal;
      if(ParseTradingExecutionSignalFields(signal_text, signal, command_count, request_id, message_type, response_symbol, response_primary_timeframe))
      {
         int count = ArraySize(signals_out);
         ArrayResize(signals_out, count + 1);
         signals_out[count] = signal;
         parsed_count++;
      }
      command_count++;
      position = next_position;
   }
   gTradingCommandTraceCsvLogger.Record("COMMAND_LIST_PARSE_RESULT", request_id, gPendingTradingExecutionRequestId, message_type, response_symbol, gPendingTradingExecutionSymbol, response_primary_timeframe, gPendingTradingExecutionTimeframe, -1, "", "", "", "", "", "", command_count, parsed_count, "ok", "");
   return true;
}

bool ParseDurationProfileLevelPayload(const string json_text, SDurationProfileLevel &level)
{
   if(!ExtractJsonDouble(json_text, "price", level.price))
      return false;
   if(!ExtractJsonLong(json_text, "duration_ms", level.duration_ms))
      return false;
   if(!ExtractJsonDouble(json_text, "duration_fraction", level.duration_fraction))
      level.duration_fraction = 0.0;
   if(!ExtractJsonBool(json_text, "significant", level.significant))
      level.significant = false;
   return true;
}

bool ParseDurationProfilePayload(
   const string json_text,
   SDurationProfile &profile,
   SDurationProfileLevel &levels_out[]
)
{
   ArrayResize(levels_out, 0);

   string message_type = "";
   if(!ExtractJsonString(json_text, "type", message_type))
      return false;
   if(message_type != "DURATION_PROFILE")
      return false;

   if(!ExtractJsonString(json_text, "symbol", profile.symbol))
      return false;
   if(profile.symbol != BuildRuntimeSymbol())
      return false;
   if(!ExtractJsonString(json_text, "timeframe", profile.timeframe))
      return false;
   if(!IsSupportedMt5AbsorptionTimeframe(profile.timeframe))
      return false;
   if(!ExtractJsonLong(json_text, "candle_open_time_utc_ms", profile.candle_open_time_utc_ms))
      return false;
   if(!ExtractJsonLong(json_text, "candle_close_time_utc_ms", profile.candle_close_time_utc_ms))
      return false;
   if(!ExtractJsonLong(json_text, "candle_duration_ms", profile.candle_duration_ms))
      return false;
   if(!ExtractJsonDouble(json_text, "price_step", profile.price_step))
      return false;
   if(!ExtractJsonLong(json_text, "max_duration_ms", profile.max_duration_ms))
      return false;

   string levels_text = "";
   if(!ExtractJsonArray(json_text, "levels", levels_text))
      return false;

   int position = 0;
   while(position < StringLen(levels_text))
   {
      string level_text = "";
      int next_position = position;
      if(!ExtractNextJsonObject(levels_text, position, level_text, next_position))
         break;

      SDurationProfileLevel level;
      if(ParseDurationProfileLevelPayload(level_text, level))
      {
         int count = ArraySize(levels_out);
         ArrayResize(levels_out, count + 1);
         levels_out[count] = level;
      }
      position = next_position;
   }

   return true;
}

bool ParseLevelVolumeProfileLevelPayload(const string json_text, SLevelVolumeProfileLevel &level)
{
   if(!ExtractJsonDouble(json_text, "p", level.price) && !ExtractJsonDouble(json_text, "price", level.price))
      return false;
   if(!ExtractJsonDouble(json_text, "b", level.agg_buy_volume) && !ExtractJsonDouble(json_text, "agg_buy_volume", level.agg_buy_volume))
      return false;
   if(!ExtractJsonDouble(json_text, "s", level.agg_sell_volume) && !ExtractJsonDouble(json_text, "agg_sell_volume", level.agg_sell_volume))
      return false;
   if(!ExtractJsonDouble(json_text, "t", level.total_volume) && !ExtractJsonDouble(json_text, "total_volume", level.total_volume))
      level.total_volume = level.agg_buy_volume + level.agg_sell_volume;
   if(!ExtractJsonDouble(json_text, "d", level.delta_volume) && !ExtractJsonDouble(json_text, "delta_volume", level.delta_volume))
      level.delta_volume = level.agg_buy_volume - level.agg_sell_volume;
   if(!ExtractJsonDouble(json_text, "volume_fraction", level.volume_fraction))
      level.volume_fraction = 0.0;
   return true;
}

bool ParseLevelVolumeProfilePayload(
   const string json_text,
   SLevelVolumeProfile &profile,
   SLevelVolumeProfileLevel &levels_out[]
)
{
   ArrayResize(levels_out, 0);

   string message_type = "";
   if(!ExtractJsonString(json_text, "type", message_type))
      return false;
   if(message_type != "LEVEL_VOLUME_PROFILE")
      return false;

   if(!ExtractJsonString(json_text, "symbol", profile.symbol))
      return false;
   if(profile.symbol != BuildRuntimeSymbol())
      return false;
   if(!ExtractJsonString(json_text, "timeframe", profile.timeframe))
      return false;
   if(!IsSupportedMt5AbsorptionTimeframe(profile.timeframe))
      return false;
   if(!ExtractJsonLong(json_text, "candle_open_time_utc_ms", profile.candle_open_time_utc_ms))
      return false;
   if(!ExtractJsonLong(json_text, "candle_close_time_utc_ms", profile.candle_close_time_utc_ms))
      return false;
   if(!ExtractJsonDouble(json_text, "price_step", profile.price_step))
      return false;
   if(!ExtractJsonDouble(json_text, "max_total_volume", profile.max_total_volume))
      return false;
   if(!ExtractJsonLong(json_text, "levels_count", profile.levels_count))
      profile.levels_count = 0;

   string levels_text = "";
   if(!ExtractJsonArray(json_text, "levels", levels_text))
      return false;

   int position = 0;
   while(position < StringLen(levels_text))
   {
      string level_text = "";
      int next_position = position;
      if(!ExtractNextJsonObject(levels_text, position, level_text, next_position))
         break;

      SLevelVolumeProfileLevel level;
      if(ParseLevelVolumeProfileLevelPayload(level_text, level))
      {
         int count = ArraySize(levels_out);
         ArrayResize(levels_out, count + 1);
         levels_out[count] = level;
      }
      position = next_position;
   }

   return true;
}

bool ParseVolumeZScoreProfileBinPayload(const string json_text, SVolumeZScoreProfileBin &bin)
{
   if(!ExtractJsonDouble(json_text, "bin_low", bin.bin_low))
      return false;
   if(!ExtractJsonDouble(json_text, "bin_high", bin.bin_high))
      return false;
   if(!ExtractJsonDouble(json_text, "current_volume", bin.current_volume))
      return false;
   if(!ExtractJsonDouble(json_text, "current_buy_volume", bin.current_buy_volume))
      bin.current_buy_volume = 0.0;
   if(!ExtractJsonDouble(json_text, "current_sell_volume", bin.current_sell_volume))
      bin.current_sell_volume = 0.0;
   if(!ExtractJsonDouble(json_text, "current_delta_volume", bin.current_delta_volume))
      bin.current_delta_volume = bin.current_buy_volume - bin.current_sell_volume;
   if(!ExtractJsonLong(json_text, "baseline_count", bin.baseline_count))
      bin.baseline_count = 0;
   if(!ExtractJsonDouble(json_text, "baseline_median_volume", bin.baseline_median_volume))
      bin.baseline_median_volume = 0.0;
   if(!ExtractJsonDouble(json_text, "baseline_mad_volume", bin.baseline_mad_volume))
      bin.baseline_mad_volume = 0.0;
   if(!ExtractJsonDouble(json_text, "effective_mad_volume", bin.effective_mad_volume))
      bin.effective_mad_volume = 0.0;
   if(!ExtractJsonDouble(json_text, "volume_z_score", bin.volume_z_score))
      return false;
   if(!ExtractJsonDouble(json_text, "positive_volume_z_score", bin.positive_volume_z_score))
      bin.positive_volume_z_score = MathMax(bin.volume_z_score, 0.0);
   if(!ExtractJsonDouble(json_text, "z_cap", bin.z_cap))
      bin.z_cap = 5.0;
   if(!ExtractJsonDouble(json_text, "line_width_ratio", bin.line_width_ratio))
      bin.line_width_ratio = 0.0;
   return true;
}

bool ParseVolumeZScoreProfilePayload(
   const string json_text,
   SVolumeZScoreProfile &profile,
   SVolumeZScoreProfileBin &bins_out[]
)
{
   ArrayResize(bins_out, 0);

   string message_type = "";
   if(!ExtractJsonString(json_text, "type", message_type))
      return false;
   if(message_type != "VOLUME_ZSCORE_PROFILE")
      return false;

   if(!ExtractJsonString(json_text, "symbol", profile.symbol))
      return false;
   if(profile.symbol != BuildRuntimeSymbol())
      return false;
   if(!ExtractJsonString(json_text, "timeframe", profile.timeframe))
      return false;
   if(!IsSupportedMt5AbsorptionTimeframe(profile.timeframe))
      return false;
   if(!ExtractJsonLong(json_text, "candle_open_time_utc_ms", profile.candle_open_time_utc_ms))
      return false;
   if(!ExtractJsonLong(json_text, "candle_close_time_utc_ms", profile.candle_close_time_utc_ms))
      return false;
   if(!ExtractJsonDouble(json_text, "fixed_bin_size", profile.fixed_bin_size))
      return false;
   if(!ExtractJsonLong(json_text, "baseline_count", profile.baseline_count))
      profile.baseline_count = 0;
   if(!ExtractJsonDouble(json_text, "z_cap", profile.z_cap))
      profile.z_cap = 5.0;
   if(!ExtractJsonDouble(json_text, "max_positive_volume_z_score", profile.max_positive_volume_z_score))
      profile.max_positive_volume_z_score = 0.0;
   if(!ExtractJsonLong(json_text, "bins_count", profile.bins_count))
      profile.bins_count = 0;

   string bins_text = "";
   if(!ExtractJsonArray(json_text, "bins", bins_text))
      return false;

   int position = 0;
   while(position < StringLen(bins_text))
   {
      string bin_text = "";
      int next_position = position;
      if(!ExtractNextJsonObject(bins_text, position, bin_text, next_position))
         break;

      SVolumeZScoreProfileBin bin;
      if(ParseVolumeZScoreProfileBinPayload(bin_text, bin))
      {
         int count = ArraySize(bins_out);
         ArrayResize(bins_out, count + 1);
         bins_out[count] = bin;
      }
      position = next_position;
   }

   return true;
}

bool ReceiveExpectedType(const string expected_type, const int timeout_ms, string &message_text_out)
{
   message_text_out = "";
   ulong start_ms = GetTickCount64();
   while((int)(GetTickCount64() - start_ms) <= timeout_ms)
   {
      string incoming_message = "";
      if(!gTcpBridgeClient.ReceiveLineWithTimeout(50, incoming_message))
         continue;
      if(StringFind(incoming_message, "\"type\":\"" + expected_type + "\"") >= 0)
      {
         message_text_out = incoming_message;
         return true;
      }
   }
   return false;
}

bool SendCommandExpectingResponse(const string request_message, const string expected_type, string &response_message)
{
   response_message = "";
   if(!gTcpBridgeClient.SendLine(request_message))
      return false;
   return ReceiveExpectedType(expected_type, InpCommandRecvTimeoutMs, response_message);
}

bool SendDurationProfileRequest(const string request_id, const string symbol, const string timeframe)
{
   string request_message = StringFormat(
      "{\"type\":\"GET_DURATION_PROFILE\",\"request_id\":\"%s\",\"symbol\":\"%s\",\"timeframe\":\"%s\",\"generated_at_utc\":%I64d}",
      EscapeJson(request_id),
      EscapeJson(symbol),
      EscapeJson(timeframe),
      (long)TimeGMT() * 1000
   );
   return gTcpBridgeClient.SendLine(request_message);
}

bool SendLevelVolumeProfileRequest(const string request_id, const string symbol, const string timeframe)
{
   string request_message = StringFormat(
      "{\"type\":\"GET_LEVEL_VOLUME_PROFILE\",\"request_id\":\"%s\",\"symbol\":\"%s\",\"timeframe\":\"%s\",\"generated_at_utc\":%I64d}",
      EscapeJson(request_id),
      EscapeJson(symbol),
      EscapeJson(timeframe),
      (long)TimeGMT() * 1000
   );
   return gTcpBridgeClient.SendLine(request_message);
}

bool SendVolumeZScoreProfileRequest(const string request_id, const string symbol, const string timeframe)
{
   string request_message = StringFormat(
      "{\"type\":\"GET_VOLUME_ZSCORE_PROFILE\",\"request_id\":\"%s\",\"symbol\":\"%s\",\"timeframe\":\"%s\",\"generated_at_utc\":%I64d}",
      EscapeJson(request_id),
      EscapeJson(symbol),
      EscapeJson(timeframe),
      (long)TimeGMT() * 1000
   );
   return gTcpBridgeClient.SendLine(request_message);
}

bool SendTradingExecutionSignalRequest(const string request_id, const string symbol, const string primary_timeframe)
{
   string request_message = StringFormat(
      "{\"type\":\"GET_TRADING_EXECUTION_SIGNALS\",\"request_id\":\"%s\",\"client_name\":\"metatrader\",\"symbol\":\"%s\",\"primary_timeframe\":\"%s\",\"generated_at_utc\":%I64d}",
      EscapeJson(request_id),
      EscapeJson(symbol),
      EscapeJson(primary_timeframe),
      (long)TimeGMT() * 1000
   );
   return gTcpBridgeClient.SendLine(request_message);
}

bool SendTradingPositionStatusUpdate(const STradingPositionStatusUpdate &status_update)
{
   string request_id = BuildRequestId("UPDATE_TRADING_POSITION_STATUS", status_update.symbol_name, status_update.timeframe);
   string request_message = StringFormat(
      "{\"type\":\"UPDATE_TRADING_POSITION_STATUS\",\"request_id\":\"%s\",\"execution_request_id\":\"%s\",\"client_name\":\"%s\",\"position_id\":\"%s\",\"client_position_id\":\"%s\",\"client_position_identifier\":\"%s\",\"symbol_name\":\"%s\",\"timeframe\":\"%s\",\"side\":\"%s\",\"status\":\"%s\",\"signal_time\":%I64d,\"cluster_id\":\"%s\",\"profit\":%.8f,\"entry_price\":%.8f,\"opened_at_utc_ms\":%I64d,\"rejection_reason\":\"%s\",\"generated_at_utc\":%I64d}",
      EscapeJson(request_id),
      EscapeJson(status_update.request_id),
      EscapeJson(status_update.client_name),
      EscapeJson(status_update.position_id),
      EscapeJson(status_update.client_position_id),
      EscapeJson(status_update.client_position_identifier),
      EscapeJson(status_update.symbol_name),
      EscapeJson(status_update.timeframe),
      EscapeJson(ExecutionSideToText(status_update.side)),
      EscapeJson(status_update.status),
      status_update.signal_time_utc_ms,
      EscapeJson(status_update.cluster_id),
      status_update.profit,
      status_update.entry_price,
      status_update.opened_at_utc_ms,
      EscapeJson(status_update.rejection_reason),
      (long)TimeGMT() * 1000
   );
   if(!gTcpBridgeClient.SendLine(request_message))
      return false;

   string response_message = "";
   if(!gTcpBridgeClient.ReceiveLineWithTimeout(InpExecutionStatusRecvTimeoutMs, response_message))
      return false;

   string message_type = "";
   if(ExtractMessageType(response_message, message_type) && IsTradingPositionStatusAckType(message_type))
      return true;

   StoreInboxMessageIfPending(response_message);
   return false;
}

int FindTrackedPositionIndex(const string position_id)
{
   for(int i = 0; i < ArraySize(gTrackedPositionIds); i++)
   {
      if(gTrackedPositionIds[i] == position_id)
         return i;
   }
   return -1;
}

int FindTrackedPositionIndexByClientId(const string client_position_id)
{
   if(client_position_id == "")
      return -1;
   for(int i = 0; i < ArraySize(gTrackedPositionClientIds); i++)
   {
      if(gTrackedPositionClientIds[i] == client_position_id)
         return i;
   }
   return -1;
}

bool PositionMatchesTrackedIdentity(
   const ulong ticket,
   const long position_identifier,
   const string client_position_id,
   const long tracked_identifier
)
{
   if(client_position_id != "" && client_position_id == IntegerToString((long)ticket))
      return true;
   return (tracked_identifier > 0 && position_identifier == tracked_identifier);
}

bool FindOpenPositionIdentity(
   const string client_position_id,
   const string symbol,
   const ENUM_EXECUTION_SIDE side,
   long &position_identifier_out
)
{
   position_identifier_out = 0;
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
      if(client_position_id != "" && client_position_id != IntegerToString((long)ticket))
         continue;
      position_identifier_out = PositionGetInteger(POSITION_IDENTIFIER);
      return true;
   }
   return false;
}

void RemoveTrackedPositionAt(const int index)
{
   int count = ArraySize(gTrackedPositionIds);
   if(index < 0 || index >= count)
      return;
   for(int i = index + 1; i < count; i++)
   {
      gTrackedPositionRequestIds[i - 1] = gTrackedPositionRequestIds[i];
      gTrackedPositionIds[i - 1] = gTrackedPositionIds[i];
      gTrackedPositionClientIds[i - 1] = gTrackedPositionClientIds[i];
      gTrackedPositionSymbols[i - 1] = gTrackedPositionSymbols[i];
      gTrackedPositionTimeframes[i - 1] = gTrackedPositionTimeframes[i];
      gTrackedPositionSides[i - 1] = gTrackedPositionSides[i];
      gTrackedPositionIdentifiers[i - 1] = gTrackedPositionIdentifiers[i];
   }
   ArrayResize(gTrackedPositionRequestIds, count - 1);
   ArrayResize(gTrackedPositionIds, count - 1);
   ArrayResize(gTrackedPositionClientIds, count - 1);
   ArrayResize(gTrackedPositionSymbols, count - 1);
   ArrayResize(gTrackedPositionTimeframes, count - 1);
   ArrayResize(gTrackedPositionSides, count - 1);
   ArrayResize(gTrackedPositionIdentifiers, count - 1);
}

void TrackPositionStatusUpdate(const STradingPositionStatusUpdate &status_update)
{
   if(status_update.position_id == "" && status_update.client_position_id == "")
      return;

   if(status_update.status == "POSITION_OPENED" || status_update.status == "POSITION_STILL_OPEN")
   {
      int index = FindTrackedPositionIndex(status_update.position_id);
      if(index < 0)
         index = FindTrackedPositionIndexByClientId(status_update.client_position_id);
      if(index < 0)
      {
         index = ArraySize(gTrackedPositionIds);
         ArrayResize(gTrackedPositionRequestIds, index + 1);
         ArrayResize(gTrackedPositionIds, index + 1);
         ArrayResize(gTrackedPositionClientIds, index + 1);
         ArrayResize(gTrackedPositionSymbols, index + 1);
         ArrayResize(gTrackedPositionTimeframes, index + 1);
         ArrayResize(gTrackedPositionSides, index + 1);
         ArrayResize(gTrackedPositionIdentifiers, index + 1);
      }
      gTrackedPositionRequestIds[index] = status_update.request_id;
      gTrackedPositionIds[index] = status_update.position_id;
      gTrackedPositionClientIds[index] = status_update.client_position_id;
      gTrackedPositionSymbols[index] = status_update.symbol_name;
      gTrackedPositionTimeframes[index] = status_update.timeframe;
      gTrackedPositionSides[index] = (int)status_update.side;
      long position_identifier = 0;
      if(status_update.client_position_identifier != "")
         position_identifier = (long)StringToInteger(status_update.client_position_identifier);
      if(position_identifier <= 0)
         FindOpenPositionIdentity(status_update.client_position_id, status_update.symbol_name, status_update.side, position_identifier);
      gTrackedPositionIdentifiers[index] = position_identifier;
      return;
   }

   if(status_update.status == "POSITION_CLOSED_BY_STOP_LOSS" || status_update.status == "POSITION_CLOSED_BY_SIGNAL")
   {
      int index = FindTrackedPositionIndex(status_update.position_id);
      if(index < 0)
         index = FindTrackedPositionIndexByClientId(status_update.client_position_id);
      if(index >= 0)
         RemoveTrackedPositionAt(index);
   }
}

bool OpenTrackedPositionStillExists(
   const string client_position_id,
   const string symbol,
   const ENUM_EXECUTION_SIDE side,
   const long tracked_identifier
)
{
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
      long position_identifier = PositionGetInteger(POSITION_IDENTIFIER);
      if(PositionMatchesTrackedIdentity(ticket, position_identifier, client_position_id, tracked_identifier))
         return true;
   }
   return false;
}

bool LoadTrackedOpenPositionSnapshot(
   const string client_position_id,
   const string symbol,
   const ENUM_EXECUTION_SIDE side,
   const long tracked_identifier,
   string &client_position_id_out,
   string &client_position_identifier_out,
   double &profit_out,
   double &entry_price_out,
   long &opened_at_utc_ms_out
)
{
   client_position_id_out = "";
   client_position_identifier_out = "";
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
      long position_identifier = PositionGetInteger(POSITION_IDENTIFIER);
      if(!PositionMatchesTrackedIdentity(ticket, position_identifier, client_position_id, tracked_identifier))
         continue;

      client_position_id_out = IntegerToString((long)ticket);
      client_position_identifier_out = IntegerToString(position_identifier);
      profit_out = PositionGetDouble(POSITION_PROFIT);
      entry_price_out = PositionGetDouble(POSITION_PRICE_OPEN);
      opened_at_utc_ms_out = ((long)PositionGetInteger(POSITION_TIME)) * 1000;
      return true;
   }
   return false;
}

void ReportTrackedOpenPositionStatuses()
{
   for(int i = 0; i < ArraySize(gTrackedPositionIds); i++)
   {
      string request_id = gTrackedPositionRequestIds[i];
      string position_id = gTrackedPositionIds[i];
      string tracked_client_position_id = gTrackedPositionClientIds[i];
      string symbol = gTrackedPositionSymbols[i];
      string timeframe = gTrackedPositionTimeframes[i];
      ENUM_EXECUTION_SIDE side = (ENUM_EXECUTION_SIDE)gTrackedPositionSides[i];
      long tracked_identifier = gTrackedPositionIdentifiers[i];
      string client_position_id = "";
      string client_position_identifier = "";
      double profit = 0.0;
      double entry_price = 0.0;
      long opened_at_utc_ms = 0;
      if(!LoadTrackedOpenPositionSnapshot(tracked_client_position_id, symbol, side, tracked_identifier, client_position_id, client_position_identifier, profit, entry_price, opened_at_utc_ms))
         continue;
      gTrackedPositionClientIds[i] = client_position_id;
      if(client_position_identifier != "")
         gTrackedPositionIdentifiers[i] = (long)StringToInteger(client_position_identifier);

      STradingPositionStatusUpdate status_update;
      status_update.client_name = "metatrader";
      status_update.request_id = request_id;
      status_update.position_id = position_id;
      status_update.client_position_id = client_position_id;
      status_update.client_position_identifier = client_position_identifier;
      status_update.symbol_name = symbol;
      status_update.timeframe = timeframe;
      status_update.side = side;
      status_update.status = "POSITION_STILL_OPEN";
      status_update.signal_time_utc_ms = 0;
      status_update.cluster_id = request_id != "" ? request_id : position_id;
      status_update.profit = profit;
      status_update.entry_price = entry_price;
      status_update.opened_at_utc_ms = opened_at_utc_ms;
      status_update.rejection_reason = "";
      SendTradingPositionStatusUpdate(status_update);
   }
}

string ClosedPositionStatusFromHistory(
   const string position_id,
   const string symbol,
   const long position_identifier,
   double &stop_hit_price,
   datetime &broker_time
)
{
   stop_hit_price = 0.0;
   broker_time = TimeCurrent();
   if(!HistorySelect(0, TimeCurrent()))
      return "";

   for(int i = HistoryDealsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0)
         continue;
      if(HistoryDealGetString(ticket, DEAL_SYMBOL) != symbol)
         continue;
      long deal_position_identifier = HistoryDealGetInteger(ticket, DEAL_POSITION_ID);
      if(position_identifier <= 0 || deal_position_identifier != position_identifier)
         continue;
      long entry = HistoryDealGetInteger(ticket, DEAL_ENTRY);
      if(entry != DEAL_ENTRY_OUT && entry != DEAL_ENTRY_INOUT && entry != DEAL_ENTRY_OUT_BY)
         continue;
      long reason = HistoryDealGetInteger(ticket, DEAL_REASON);
      if(reason == DEAL_REASON_SL)
      {
         stop_hit_price = HistoryDealGetDouble(ticket, DEAL_PRICE);
         broker_time = (datetime)HistoryDealGetInteger(ticket, DEAL_TIME);
         return "POSITION_CLOSED_BY_STOP_LOSS";
      }
      return "";
   }

   return "";
}

void MonitorTrackedPositionClosures()
{
   for(int i = ArraySize(gTrackedPositionIds) - 1; i >= 0; i--)
   {
      string request_id = gTrackedPositionRequestIds[i];
      string position_id = gTrackedPositionIds[i];
      string client_position_id = gTrackedPositionClientIds[i];
      string symbol = gTrackedPositionSymbols[i];
      string timeframe = gTrackedPositionTimeframes[i];
      long position_identifier = gTrackedPositionIdentifiers[i];
      ENUM_EXECUTION_SIDE side = (ENUM_EXECUTION_SIDE)gTrackedPositionSides[i];
      if(OpenTrackedPositionStillExists(client_position_id, symbol, side, position_identifier))
         continue;

      double stop_hit_price = 0.0;
      datetime broker_time = TimeCurrent();
      string close_status = ClosedPositionStatusFromHistory(position_id, symbol, position_identifier, stop_hit_price, broker_time);
      if(close_status != "POSITION_CLOSED_BY_STOP_LOSS")
      {
         PrintFormat(
            "TRACKED_POSITION_MISSING_WITHOUT_STOP_LOSS | position_id=%s | symbol=%s | timeframe=%s | side=%s | identifier=%I64d",
            position_id,
            symbol,
            timeframe,
            ExecutionSideToText(side),
            position_identifier
         );
         continue;
      }

      gMetaTraderExecutionClient.RegisterStopLossClosure(symbol, timeframe, side, stop_hit_price, broker_time);

      STradingPositionStatusUpdate status_update;
      status_update.client_name = "metatrader";
      status_update.request_id = request_id;
      status_update.position_id = position_id;
      status_update.client_position_id = client_position_id;
      status_update.client_position_identifier = IntegerToString(position_identifier);
      status_update.symbol_name = symbol;
      status_update.timeframe = timeframe;
      status_update.side = side;
      status_update.status = close_status;
      status_update.signal_time_utc_ms = 0;
      status_update.cluster_id = request_id != "" ? request_id : position_id;
      status_update.profit = 0.0;
      status_update.entry_price = 0.0;
      status_update.opened_at_utc_ms = 0;
      status_update.rejection_reason = "";
      SendTradingPositionStatusUpdate(status_update);
      RemoveTrackedPositionAt(i);
   }
}

bool SendSystemInit()
{
   int symbol_count = SymbolsTotal(true);
   string runtime_symbol = BuildRuntimeSymbol();
<<<<<<< HEAD
=======
   string trading_timeframe = TradingTimeframeToText(InpTradingTimeframe);
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
   bool runtime_symbol_seen = false;
   string symbols_json = "[";

   for(int i = 0; i < symbol_count; i++)
   {
      string mt5_symbol = SymbolName(i, true);
      if(mt5_symbol == runtime_symbol)
         runtime_symbol_seen = true;
      if(i > 0)
         symbols_json += ",";
      symbols_json += StringFormat(
<<<<<<< HEAD
         "{\"mt5_symbol\":\"%s\"}",
         EscapeJson(mt5_symbol)
=======
         "{\"mt5_symbol\":\"%s\",\"timeframe\":\"%s\"}",
         EscapeJson(mt5_symbol),
         EscapeJson(trading_timeframe)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
      );
   }

   if(!runtime_symbol_seen)
   {
      if(symbol_count > 0)
         symbols_json += ",";
      symbols_json += StringFormat(
<<<<<<< HEAD
         "{\"mt5_symbol\":\"%s\"}",
         EscapeJson(runtime_symbol)
=======
         "{\"mt5_symbol\":\"%s\",\"timeframe\":\"%s\"}",
         EscapeJson(runtime_symbol),
         EscapeJson(trading_timeframe)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
      );
      symbol_count++;
   }

   symbols_json += "]";

   string request_message = StringFormat(
      "{\"type\":\"SYSTEM_INIT\",\"schema_version\":\"7.0\",\"symbols_count\":%d,\"symbols\":%s,\"generated_at_utc\":%I64d}",
      symbol_count,
      symbols_json,
      (long)TimeGMT() * 1000
   );

   string response_message = "";
<<<<<<< HEAD
   if(!SendCommandExpectingResponse(request_message, "SYSTEM_INIT_ACK", response_message))
      return false;

   string primary_execution_timeframe = "";
   if(!ExtractTopLevelJsonString(response_message, "primary_execution_timeframe", primary_execution_timeframe))
      return false;
   if(!IsSupportedMt5AbsorptionTimeframe(primary_execution_timeframe))
      return false;

   gPrimaryExecutionTimeframe = primary_execution_timeframe;
   return true;
=======
   return SendCommandExpectingResponse(request_message, "SYSTEM_INIT_ACK", response_message);
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
}

string AbsorptionObjectSafeId(const string source_text)
{
   string safe_text = source_text;
   StringReplace(safe_text, " ", "_");
   StringReplace(safe_text, "\t", "_");
   StringReplace(safe_text, "\r", "_");
   StringReplace(safe_text, "\n", "_");
   StringReplace(safe_text, "\"", "_");
   return safe_text;
}

string DurationProfileObjectKey(const string symbol, const string timeframe)
{
   return AbsorptionObjectSafeId(symbol + "_" + timeframe);
}

void RemoveDurationProfileObjectAt(const int index)
{
   int count = ArraySize(gDurationProfileObjectNames);
   if(index < 0 || index >= count)
      return;

   for(int i = index + 1; i < count; i++)
   {
      gDurationProfileObjectNames[i - 1] = gDurationProfileObjectNames[i];
      gDurationProfileObjectKeys[i - 1] = gDurationProfileObjectKeys[i];
      gDurationProfileObjectChartIds[i - 1] = gDurationProfileObjectChartIds[i];
   }

   ArrayResize(gDurationProfileObjectNames, count - 1);
   ArrayResize(gDurationProfileObjectKeys, count - 1);
   ArrayResize(gDurationProfileObjectChartIds, count - 1);
}

void RegisterDurationProfileObject(const string profile_key, const long chart_id, const string object_name)
{
   int count = ArraySize(gDurationProfileObjectNames);
   ArrayResize(gDurationProfileObjectNames, count + 1);
   ArrayResize(gDurationProfileObjectKeys, count + 1);
   ArrayResize(gDurationProfileObjectChartIds, count + 1);
   gDurationProfileObjectNames[count] = object_name;
   gDurationProfileObjectKeys[count] = profile_key;
   gDurationProfileObjectChartIds[count] = chart_id;
}

void ClearDurationProfileObjects(const string profile_key = "")
{
   for(int i = ArraySize(gDurationProfileObjectNames) - 1; i >= 0; i--)
   {
      if(profile_key != "" && gDurationProfileObjectKeys[i] != profile_key)
         continue;
      if(gDurationProfileObjectChartIds[i] >= 0 && gDurationProfileObjectNames[i] != "")
         ObjectDelete(gDurationProfileObjectChartIds[i], gDurationProfileObjectNames[i]);
      RemoveDurationProfileObjectAt(i);
   }
}

string LevelVolumeProfileObjectKey(const string symbol, const string timeframe)
{
   return AbsorptionObjectSafeId(symbol + "_" + timeframe);
}

void RemoveLevelVolumeProfileObjectAt(const int index)
{
   int count = ArraySize(gLevelVolumeProfileObjectNames);
   if(index < 0 || index >= count)
      return;

   for(int i = index + 1; i < count; i++)
   {
      gLevelVolumeProfileObjectNames[i - 1] = gLevelVolumeProfileObjectNames[i];
      gLevelVolumeProfileObjectKeys[i - 1] = gLevelVolumeProfileObjectKeys[i];
      gLevelVolumeProfileObjectChartIds[i - 1] = gLevelVolumeProfileObjectChartIds[i];
   }

   ArrayResize(gLevelVolumeProfileObjectNames, count - 1);
   ArrayResize(gLevelVolumeProfileObjectKeys, count - 1);
   ArrayResize(gLevelVolumeProfileObjectChartIds, count - 1);
}

void RegisterLevelVolumeProfileObject(const string profile_key, const long chart_id, const string object_name)
{
   int count = ArraySize(gLevelVolumeProfileObjectNames);
   ArrayResize(gLevelVolumeProfileObjectNames, count + 1);
   ArrayResize(gLevelVolumeProfileObjectKeys, count + 1);
   ArrayResize(gLevelVolumeProfileObjectChartIds, count + 1);
   gLevelVolumeProfileObjectNames[count] = object_name;
   gLevelVolumeProfileObjectKeys[count] = profile_key;
   gLevelVolumeProfileObjectChartIds[count] = chart_id;
}

void ClearLevelVolumeProfileObjects(const string profile_key = "")
{
   for(int i = ArraySize(gLevelVolumeProfileObjectNames) - 1; i >= 0; i--)
   {
      if(profile_key != "" && gLevelVolumeProfileObjectKeys[i] != profile_key)
         continue;
      if(gLevelVolumeProfileObjectChartIds[i] >= 0 && gLevelVolumeProfileObjectNames[i] != "")
         ObjectDelete(gLevelVolumeProfileObjectChartIds[i], gLevelVolumeProfileObjectNames[i]);
      RemoveLevelVolumeProfileObjectAt(i);
   }
}

string VolumeZScoreProfileObjectKey(const string symbol, const string timeframe)
{
   return AbsorptionObjectSafeId(symbol + "_" + timeframe);
}

void RemoveVolumeZScoreProfileObjectAt(const int index)
{
   int count = ArraySize(gVolumeZScoreProfileObjectNames);
   if(index < 0 || index >= count)
      return;

   for(int i = index + 1; i < count; i++)
   {
      gVolumeZScoreProfileObjectNames[i - 1] = gVolumeZScoreProfileObjectNames[i];
      gVolumeZScoreProfileObjectKeys[i - 1] = gVolumeZScoreProfileObjectKeys[i];
      gVolumeZScoreProfileObjectChartIds[i - 1] = gVolumeZScoreProfileObjectChartIds[i];
   }

   ArrayResize(gVolumeZScoreProfileObjectNames, count - 1);
   ArrayResize(gVolumeZScoreProfileObjectKeys, count - 1);
   ArrayResize(gVolumeZScoreProfileObjectChartIds, count - 1);
}

void RegisterVolumeZScoreProfileObject(const string profile_key, const long chart_id, const string object_name)
{
   int count = ArraySize(gVolumeZScoreProfileObjectNames);
   ArrayResize(gVolumeZScoreProfileObjectNames, count + 1);
   ArrayResize(gVolumeZScoreProfileObjectKeys, count + 1);
   ArrayResize(gVolumeZScoreProfileObjectChartIds, count + 1);
   gVolumeZScoreProfileObjectNames[count] = object_name;
   gVolumeZScoreProfileObjectKeys[count] = profile_key;
   gVolumeZScoreProfileObjectChartIds[count] = chart_id;
}

void ClearVolumeZScoreProfileObjects(const string profile_key = "")
{
   for(int i = ArraySize(gVolumeZScoreProfileObjectNames) - 1; i >= 0; i--)
   {
      if(profile_key != "" && gVolumeZScoreProfileObjectKeys[i] != profile_key)
         continue;
      if(gVolumeZScoreProfileObjectChartIds[i] >= 0 && gVolumeZScoreProfileObjectNames[i] != "")
         ObjectDelete(gVolumeZScoreProfileObjectChartIds[i], gVolumeZScoreProfileObjectNames[i]);
      RemoveVolumeZScoreProfileObjectAt(i);
   }
}

ENUM_TIMEFRAMES TimeframeTextToPeriod(const string timeframe)
{
   if(timeframe == "M1")  return PERIOD_M1;
   if(timeframe == "M2")  return PERIOD_M2;
   if(timeframe == "M4")  return PERIOD_M4;
   if(timeframe == "M5")  return PERIOD_M5;
   if(timeframe == "M10") return PERIOD_M10;
   if(timeframe == "M15") return PERIOD_M15;
   if(timeframe == "M30") return PERIOD_M30;
   if(timeframe == "H1")  return PERIOD_H1;
   if(timeframe == "H4")  return PERIOD_H4;
   if(timeframe == "D1")  return PERIOD_D1;
   if(timeframe == "W1")  return PERIOD_W1;
   return PERIOD_CURRENT;
}

long FindChartBySymbolTimeframe(const string symbol, const ENUM_TIMEFRAMES period)
{
   long chart_id = ChartFirst();
   while(chart_id >= 0)
   {
      if(ChartSymbol(chart_id) == symbol && (ENUM_TIMEFRAMES)ChartPeriod(chart_id) == period)
         return chart_id;
      chart_id = ChartNext(chart_id);
   }
   return -1;
}

long FindOrOpenAbsorptionChart(const string symbol, const ENUM_TIMEFRAMES period)
{
   long chart_id = FindChartBySymbolTimeframe(symbol, period);
   if(chart_id >= 0)
      return chart_id;

   ResetLastError();
   chart_id = ChartOpen(symbol, period);
   if(chart_id == 0)
      return -1;

   ChartSetSymbolPeriod(chart_id, symbol, period);
   return chart_id;
}

bool WaitForChartLoaded(const long chart_id, const string symbol, const ENUM_TIMEFRAMES period, const int timeout_ms)
{
   ulong started_ms = GetTickCount64();
   datetime loaded_times[];
   ArraySetAsSeries(loaded_times, true);
   int wait_budget_ms = timeout_ms;
   if(InpChartLoadStepTimeoutMs > 0)
      wait_budget_ms = (int)MathMin(wait_budget_ms, InpChartLoadStepTimeoutMs);
   if(wait_budget_ms < 0)
      wait_budget_ms = 0;

   while((int)(GetTickCount64() - started_ms) <= wait_budget_ms)
   {
      bool chart_matches = (ChartSymbol(chart_id) == symbol && (ENUM_TIMEFRAMES)ChartPeriod(chart_id) == period);
      bool series_synced = (bool)SeriesInfoInteger(symbol, period, SERIES_SYNCHRONIZED);
      int copied_times = CopyTime(symbol, period, 0, 2, loaded_times);
      int bars_count = Bars(symbol, period);
      if(chart_matches && series_synced && copied_times > 0 && bars_count > 0)
      {
         return true;
      }
      ChartRedraw(chart_id);
      if(wait_budget_ms <= 0)
         break;
      Sleep(20);
   }

   return false;
}

bool ReceivePendingDurationProfileResponse(const int timeout_ms, string &message_text_out)
{
   message_text_out = "";
   if(TakeInboxDurationProfileResponse(message_text_out))
      return true;

   string incoming_message = "";
   if(!gTcpBridgeClient.ReceiveLineWithTimeout(timeout_ms, incoming_message))
      return false;

   if(MessageMatchesPendingDurationProfileRequest(incoming_message))
   {
      message_text_out = incoming_message;
      return true;
   }

   StoreInboxMessageIfPending(incoming_message);
   return false;
}

bool ReceivePendingLevelVolumeProfileResponse(const int timeout_ms, string &message_text_out)
{
   message_text_out = "";
   if(TakeInboxLevelVolumeProfileResponse(message_text_out))
      return true;

   string incoming_message = "";
   if(!gTcpBridgeClient.ReceiveLineWithTimeout(timeout_ms, incoming_message))
      return false;

   if(MessageMatchesPendingLevelVolumeProfileRequest(incoming_message))
   {
      message_text_out = incoming_message;
      return true;
   }

   StoreInboxMessageIfPending(incoming_message);
   return false;
}

bool ReceivePendingVolumeZScoreProfileResponse(const int timeout_ms, string &message_text_out)
{
   message_text_out = "";
   if(TakeInboxVolumeZScoreProfileResponse(message_text_out))
      return true;

   string incoming_message = "";
   if(!gTcpBridgeClient.ReceiveLineWithTimeout(timeout_ms, incoming_message))
      return false;

   if(MessageMatchesPendingVolumeZScoreProfileRequest(incoming_message))
   {
      message_text_out = incoming_message;
      return true;
   }

   StoreInboxMessageIfPending(incoming_message);
   return false;
}

bool ReceivePendingTradingExecutionResponse(const int timeout_ms, string &message_text_out)
{
   message_text_out = "";
   if(TakeInboxTradingExecutionResponse(message_text_out))
   {
      string inbox_message_type = "";
      string inbox_request_id = "";
      string inbox_response_symbol = "";
      string inbox_response_primary_timeframe = "";
      string inbox_reason = "";
      DiagnoseTradingExecutionResponseMatch(
         message_text_out,
         inbox_message_type,
         inbox_request_id,
         inbox_response_symbol,
         inbox_response_primary_timeframe,
         inbox_reason
      );
      gTradingCommandTraceCsvLogger.Record(
         "RESPONSE_MATCHED",
         inbox_request_id,
         gPendingTradingExecutionRequestId,
         inbox_message_type,
         inbox_response_symbol,
         gPendingTradingExecutionSymbol,
         inbox_response_primary_timeframe,
         gPendingTradingExecutionTimeframe,
         -1,
         "",
         "",
         "",
         "",
         "",
         "",
         0,
         0,
         "matched",
         "INBOX"
      );
      return true;
   }

   string incoming_message = "";
   if(!gTcpBridgeClient.ReceiveLineWithTimeout(timeout_ms, incoming_message))
      return false;

   string message_type = "";
   string request_id = "";
   string response_symbol = "";
   string response_primary_timeframe = "";
   string match_reason = "";
   bool diagnosed_match = DiagnoseTradingExecutionResponseMatch(
      incoming_message,
      message_type,
      request_id,
      response_symbol,
      response_primary_timeframe,
      match_reason
   );
   gTradingCommandTraceCsvLogger.Record(
      "RESPONSE_RECEIVED",
      request_id,
      gPendingTradingExecutionRequestId,
      message_type,
      response_symbol,
      gPendingTradingExecutionSymbol,
      response_primary_timeframe,
      gPendingTradingExecutionTimeframe,
      -1,
      "",
      "",
      "",
      "",
      "",
      "",
      0,
      0,
      "received",
      ""
   );

   if(MessageMatchesPendingTradingExecutionRequest(incoming_message))
   {
      gTradingCommandTraceCsvLogger.Record(
         "RESPONSE_MATCHED",
         request_id,
         gPendingTradingExecutionRequestId,
         message_type,
         response_symbol,
         gPendingTradingExecutionSymbol,
         response_primary_timeframe,
         gPendingTradingExecutionTimeframe,
         -1,
         "",
         "",
         "",
         "",
         "",
         "",
         0,
         0,
         "matched",
         ""
      );
      message_text_out = incoming_message;
      return true;
   }

   gTradingCommandTraceCsvLogger.Record(
      "RESPONSE_NOT_MATCHED",
      request_id,
      gPendingTradingExecutionRequestId,
      message_type,
      response_symbol,
      gPendingTradingExecutionSymbol,
      response_primary_timeframe,
      gPendingTradingExecutionTimeframe,
      -1,
      "",
      "",
      "",
      "",
      "",
      "",
      0,
      0,
      "rejected",
      diagnosed_match ? "" : match_reason
   );
   StoreInboxMessageIfPending(incoming_message);
   return false;
}

void ApplyChartShiftSettings(const long chart_id)
{
   ChartSetInteger(chart_id, CHART_SHIFT, true);
   ChartSetDouble(chart_id, CHART_SHIFT_SIZE, CHART_SHIFT_SIZE_PERCENT);
   ChartNavigate(chart_id, CHART_END, 0);
   ChartRedraw(chart_id);
}

void ApplyChartShiftSettingsToAllCharts()
{
   long chart_id = ChartFirst();
   while(chart_id >= 0)
   {
      ApplyChartShiftSettings(chart_id);
      chart_id = ChartNext(chart_id);
   }
}

void ApplyAbsorptionChartSettings(const long chart_id)
{
   ApplyChartShiftSettings(chart_id);
}

datetime DurationProfileRightEdgeTime(const string symbol, const ENUM_TIMEFRAMES period)
{
   int period_seconds = PeriodSeconds(period);
   if(period_seconds <= 0)
      period_seconds = 60;

   datetime latest_bar_time = iTime(symbol, period, 0);
   if(latest_bar_time <= 0)
      latest_bar_time = TimeCurrent();

   return latest_bar_time + period_seconds * (DURATION_PROFILE_MAX_WIDTH_BARS + DURATION_PROFILE_RIGHT_OFFSET_BARS);
}

datetime LevelVolumeProfileRightEdgeTime(const string symbol, const ENUM_TIMEFRAMES period)
{
   int period_seconds = PeriodSeconds(period);
   if(period_seconds <= 0)
      period_seconds = 60;
   return DurationProfileRightEdgeTime(symbol, period) + period_seconds * (LEVEL_VOLUME_PROFILE_GAP_BARS + LEVEL_VOLUME_PROFILE_MAX_WIDTH_BARS);
}

datetime VolumeZScoreProfileRightEdgeTime(const string symbol, const ENUM_TIMEFRAMES period)
{
   int period_seconds = PeriodSeconds(period);
   if(period_seconds <= 0)
      period_seconds = 60;
   return LevelVolumeProfileRightEdgeTime(symbol, period) + period_seconds * (VOLUME_ZSCORE_PROFILE_GAP_BARS + VOLUME_ZSCORE_PROFILE_MAX_WIDTH_BARS);
}

int DurationProfileBarWidth(const SDurationProfileLevel &level)
{
   if(level.significant)
      return 4;
   return 2;
}

color DurationProfileBarColor(const SDurationProfileLevel &level)
{
   if(level.significant)
      return clrOrange;
   return clrSteelBlue;
}

ENUM_LINE_STYLE LevelVolumeProfileLineStyle(const SLevelVolumeProfileLevel &level)
{
   if(level.delta_volume < 0.0)
      return STYLE_DASH;
   return STYLE_SOLID;
}

color LevelVolumeProfileBarColor(const SLevelVolumeProfileLevel &level)
{
   if(level.delta_volume > 0.0)
      return clrSeaGreen;
   if(level.delta_volume < 0.0)
      return clrTomato;
   return clrSilver;
}

ENUM_LINE_STYLE VolumeZScoreProfileLineStyle(const SVolumeZScoreProfileBin &bin)
{
   if(bin.current_delta_volume < 0.0)
      return STYLE_DASH;
   return STYLE_SOLID;
}

color VolumeZScoreProfileBarColor(const SVolumeZScoreProfileBin &bin)
{
   if(bin.current_delta_volume > 0.0)
      return clrMediumSeaGreen;
   if(bin.current_delta_volume < 0.0)
      return clrOrangeRed;
   return clrGold;
}

void RenderDurationProfile(const SDurationProfile &profile, const SDurationProfileLevel &levels[])
{
   string profile_key = DurationProfileObjectKey(profile.symbol, profile.timeframe);
   ENUM_TIMEFRAMES chart_period = TimeframeTextToPeriod(profile.timeframe);
   if(chart_period == PERIOD_CURRENT)
      return;

   long chart_id = FindOrOpenAbsorptionChart(profile.symbol, chart_period);
   if(chart_id < 0)
      return;

   if(!WaitForChartLoaded(chart_id, profile.symbol, chart_period, InpChartLoadTimeoutMs))
      return;

   ApplyAbsorptionChartSettings(chart_id);
   ClearDurationProfileObjects(profile_key);

   if(profile.max_duration_ms <= 0)
      return;

   int period_seconds = PeriodSeconds(chart_period);
   if(period_seconds <= 0)
      period_seconds = 60;
   int max_width_seconds = period_seconds * DURATION_PROFILE_MAX_WIDTH_BARS;
   datetime right_time = DurationProfileRightEdgeTime(profile.symbol, chart_period);

   for(int i = 0; i < ArraySize(levels); i++)
   {
      SDurationProfileLevel level = levels[i];
      if(level.duration_ms <= 0)
         continue;

      double width_ratio = (double)level.duration_ms / (double)profile.max_duration_ms;
      int width_seconds = (int)MathRound(width_ratio * max_width_seconds);
      if(width_seconds < 1)
         width_seconds = 1;
      if(width_seconds > max_width_seconds)
         width_seconds = max_width_seconds;

      datetime left_time = right_time - width_seconds;
      string object_name = "DUR_PROF_" + profile_key + "_" + IntegerToString(i);

      if(ObjectCreate(chart_id, object_name, OBJ_TREND, 0, left_time, level.price, right_time, level.price))
      {
         ObjectSetInteger(chart_id, object_name, OBJPROP_COLOR, DurationProfileBarColor(level));
         ObjectSetInteger(chart_id, object_name, OBJPROP_BACK, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_RAY_LEFT, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_RAY_RIGHT, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_WIDTH, DurationProfileBarWidth(level));
         ObjectSetString(
            chart_id,
            object_name,
            OBJPROP_TOOLTIP,
            StringFormat(
               "Duration %s %s price %.8f time %.3fs",
               profile.symbol,
               profile.timeframe,
               level.price,
               (double)level.duration_ms / 1000.0
            )
         );
         RegisterDurationProfileObject(profile_key, chart_id, object_name);
      }
   }

   ChartRedraw(chart_id);
}

void RenderLevelVolumeProfile(const SLevelVolumeProfile &profile, const SLevelVolumeProfileLevel &levels[])
{
   string profile_key = LevelVolumeProfileObjectKey(profile.symbol, profile.timeframe);
   ENUM_TIMEFRAMES chart_period = TimeframeTextToPeriod(profile.timeframe);
   if(chart_period == PERIOD_CURRENT)
      return;

   long chart_id = FindOrOpenAbsorptionChart(profile.symbol, chart_period);
   if(chart_id < 0)
      return;

   if(!WaitForChartLoaded(chart_id, profile.symbol, chart_period, InpChartLoadTimeoutMs))
      return;

   ApplyAbsorptionChartSettings(chart_id);
   ClearLevelVolumeProfileObjects(profile_key);

   if(profile.max_total_volume <= 0.0)
      return;

   int period_seconds = PeriodSeconds(chart_period);
   if(period_seconds <= 0)
      period_seconds = 60;
   int max_width_seconds = period_seconds * LEVEL_VOLUME_PROFILE_MAX_WIDTH_BARS;
   datetime right_time = LevelVolumeProfileRightEdgeTime(profile.symbol, chart_period);

   for(int i = 0; i < ArraySize(levels); i++)
   {
      SLevelVolumeProfileLevel level = levels[i];
      if(level.total_volume <= 0.0)
         continue;

      double width_ratio = level.total_volume / profile.max_total_volume;
      int width_seconds = (int)MathRound(width_ratio * max_width_seconds);
      if(width_seconds < 1)
         width_seconds = 1;
      if(width_seconds > max_width_seconds)
         width_seconds = max_width_seconds;

      datetime left_time = right_time - width_seconds;
      string object_name = "VOL_PROF_" + profile_key + "_" + IntegerToString(i);

      if(ObjectCreate(chart_id, object_name, OBJ_TREND, 0, left_time, level.price, right_time, level.price))
      {
         ObjectSetInteger(chart_id, object_name, OBJPROP_COLOR, LevelVolumeProfileBarColor(level));
         ObjectSetInteger(chart_id, object_name, OBJPROP_STYLE, LevelVolumeProfileLineStyle(level));
         ObjectSetInteger(chart_id, object_name, OBJPROP_BACK, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_RAY_LEFT, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_RAY_RIGHT, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_WIDTH, 2);
         ObjectSetString(
            chart_id,
            object_name,
            OBJPROP_TOOLTIP,
            StringFormat(
               "Volume %s %s price %.8f total %.3f delta %.3f buy %.3f sell %.3f",
               profile.symbol,
               profile.timeframe,
               level.price,
               level.total_volume,
               level.delta_volume,
               level.agg_buy_volume,
               level.agg_sell_volume
            )
         );
         RegisterLevelVolumeProfileObject(profile_key, chart_id, object_name);
      }
   }

   ChartRedraw(chart_id);
}

void RenderVolumeZScoreProfile(const SVolumeZScoreProfile &profile, const SVolumeZScoreProfileBin &bins[])
{
   string profile_key = VolumeZScoreProfileObjectKey(profile.symbol, profile.timeframe);
   ENUM_TIMEFRAMES chart_period = TimeframeTextToPeriod(profile.timeframe);
   if(chart_period == PERIOD_CURRENT)
      return;

   long chart_id = FindOrOpenAbsorptionChart(profile.symbol, chart_period);
   if(chart_id < 0)
      return;

   if(!WaitForChartLoaded(chart_id, profile.symbol, chart_period, InpChartLoadTimeoutMs))
      return;

   ApplyAbsorptionChartSettings(chart_id);
   ClearVolumeZScoreProfileObjects(profile_key);

   if(profile.z_cap <= 0.0)
      return;

   int period_seconds = PeriodSeconds(chart_period);
   if(period_seconds <= 0)
      period_seconds = 60;
   int max_width_seconds = period_seconds * VOLUME_ZSCORE_PROFILE_MAX_WIDTH_BARS;
   datetime right_time = VolumeZScoreProfileRightEdgeTime(profile.symbol, chart_period);

   for(int i = 0; i < ArraySize(bins); i++)
   {
      SVolumeZScoreProfileBin bin = bins[i];
      if(bin.current_volume <= 0.0 || bin.positive_volume_z_score <= 0.0)
         continue;

      double width_ratio = bin.line_width_ratio;
      if(width_ratio <= 0.0)
         width_ratio = MathMin(bin.positive_volume_z_score / profile.z_cap, 1.0);
      if(width_ratio > 1.0)
         width_ratio = 1.0;

      int width_seconds = (int)MathRound(width_ratio * max_width_seconds);
      if(width_seconds < 1)
         width_seconds = 1;
      if(width_seconds > max_width_seconds)
         width_seconds = max_width_seconds;

      double price = (bin.bin_low + bin.bin_high) / 2.0;
      datetime left_time = right_time - width_seconds;
      string object_name = "ZVOL_PROF_" + profile_key + "_" + IntegerToString(i);

      if(ObjectCreate(chart_id, object_name, OBJ_TREND, 0, left_time, price, right_time, price))
      {
         ObjectSetInteger(chart_id, object_name, OBJPROP_COLOR, VolumeZScoreProfileBarColor(bin));
         ObjectSetInteger(chart_id, object_name, OBJPROP_STYLE, VolumeZScoreProfileLineStyle(bin));
         ObjectSetInteger(chart_id, object_name, OBJPROP_BACK, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_RAY_LEFT, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_RAY_RIGHT, false);
         ObjectSetInteger(chart_id, object_name, OBJPROP_WIDTH, 2);
         ObjectSetString(
            chart_id,
            object_name,
            OBJPROP_TOOLTIP,
            StringFormat(
               "Volume Z %s %s bin %.8f-%.8f z %.3f vol %.3f median %.3f MAD %.3f delta %.3f",
               profile.symbol,
               profile.timeframe,
               bin.bin_low,
               bin.bin_high,
               bin.volume_z_score,
               bin.current_volume,
               bin.baseline_median_volume,
               bin.baseline_mad_volume,
               bin.current_delta_volume
            )
         );
         RegisterVolumeZScoreProfileObject(profile_key, chart_id, object_name);
      }
   }

   ChartRedraw(chart_id);
}

void HandleDurationProfileResponse(const string response_message)
{
   if(StringFind(response_message, "\"type\":\"NO_DURATION_PROFILE\"") >= 0)
      return;

   SDurationProfile profile;
   SDurationProfileLevel levels[];
   if(ParseDurationProfilePayload(response_message, profile, levels))
      RenderDurationProfile(profile, levels);
}

void HandleLevelVolumeProfileResponse(const string response_message)
{
   if(StringFind(response_message, "\"type\":\"NO_LEVEL_VOLUME_PROFILE\"") >= 0)
   {
      if(gPendingLevelVolumeProfileSymbol != "" && gPendingLevelVolumeProfileTimeframe != "")
         ClearLevelVolumeProfileObjects(LevelVolumeProfileObjectKey(gPendingLevelVolumeProfileSymbol, gPendingLevelVolumeProfileTimeframe));
      return;
   }

   SLevelVolumeProfile profile;
   SLevelVolumeProfileLevel levels[];
   if(ParseLevelVolumeProfilePayload(response_message, profile, levels))
      RenderLevelVolumeProfile(profile, levels);
}

void HandleVolumeZScoreProfileResponse(const string response_message)
{
   if(StringFind(response_message, "\"type\":\"NO_VOLUME_ZSCORE_PROFILE\"") >= 0)
   {
      if(gPendingVolumeZScoreProfileSymbol != "" && gPendingVolumeZScoreProfileTimeframe != "")
         ClearVolumeZScoreProfileObjects(VolumeZScoreProfileObjectKey(gPendingVolumeZScoreProfileSymbol, gPendingVolumeZScoreProfileTimeframe));
      return;
   }

   SVolumeZScoreProfile profile;
   SVolumeZScoreProfileBin bins[];
   if(ParseVolumeZScoreProfilePayload(response_message, profile, bins))
      RenderVolumeZScoreProfile(profile, bins);
}

int DurationProfilePollMs()
{
   return 1000;
}

int DurationProfileRecvTimeoutMs()
{
   return 1;
}

int LevelVolumeProfilePollMs()
{
   return 1000;
}

int LevelVolumeProfileRecvTimeoutMs()
{
   return 25;
}

int VolumeZScoreProfilePollMs()
{
   return 1000;
}

int VolumeZScoreProfileRecvTimeoutMs()
{
   return 25;
}

int TradingExecutionSignalPollMs()
{
   if(InpExecutionSignalPollMs <= 0)
      return 1000;
   return InpExecutionSignalPollMs;
}

int TradingExecutionSignalRecvTimeoutMs()
{
   if(InpExecutionSignalRecvTimeoutMs <= 0)
      return 25;
   return InpExecutionSignalRecvTimeoutMs;
}

int RuntimeTimerMs()
{
   return TradingExecutionSignalPollMs();
}

void EvaluateTradingExecutionSignals()
{
   STradingPositionStatusUpdate status_updates[];
   gTradingStrategyEngine.Evaluate(
      BuildRuntimeSymbol(),
      gLatestTradingExecutionSignals,
      status_updates
   );

   for(int i = 0; i < ArraySize(status_updates); i++)
   {
      SendTradingPositionStatusUpdate(status_updates[i]);
      TrackPositionStatusUpdate(status_updates[i]);
   }
}

void HandleTradingExecutionSignalResponse(const string response_message)
{
   if(StringFind(response_message, "\"type\":\"NO_TRADING_EXECUTION_SIGNAL\"") >= 0 ||
      StringFind(response_message, "\"type\":\"NO_TRADING_EXECUTION_COMMAND\"") >= 0)
   {
      string no_command_type = "";
      string no_command_request_id = "";
      string no_command_symbol = "";
      string no_command_primary_timeframe = "";
      ExtractJsonString(response_message, "type", no_command_type);
      ExtractJsonString(response_message, "request_id", no_command_request_id);
      ExtractJsonString(response_message, "symbol", no_command_symbol);
      ExtractJsonString(response_message, "primary_timeframe", no_command_primary_timeframe);
      gTradingCommandTraceCsvLogger.Record(
         "NO_COMMAND_RESPONSE",
         no_command_request_id,
         gPendingTradingExecutionRequestId,
         no_command_type,
         no_command_symbol,
         gPendingTradingExecutionSymbol,
         no_command_primary_timeframe,
         gPendingTradingExecutionTimeframe,
         -1,
         "",
         "",
         "",
         "",
         "",
         "",
         0,
         0,
         "ok",
         ""
      );
      ArrayResize(gLatestTradingExecutionSignals, 0);
      return;
   }

   if(ParseTradingExecutionSignalListPayload(response_message, gLatestTradingExecutionSignals))
      EvaluateTradingExecutionSignals();
}

void PollTradingExecutionSignals()
{
   ulong now_ms = GetTickCount64();
   if(gTradingExecutionRequestPending)
   {
      string response_message = "";
      if(ReceivePendingTradingExecutionResponse(TradingExecutionSignalRecvTimeoutMs(), response_message))
      {
         HandleTradingExecutionSignalResponse(response_message);
         ClearPendingTradingExecutionRequest();
      }
      else if((int)(now_ms - gTradingExecutionRequestSentMs) > InpCommandRecvTimeoutMs)
      {
         ClearPendingTradingExecutionRequest();
      }
      return;
   }

   if((int)(now_ms - gLastTradingExecutionPollMs) < TradingExecutionSignalPollMs())
      return;

   ReportTrackedOpenPositionStatuses();

   string request_symbol = BuildRuntimeSymbol();
<<<<<<< HEAD
   string request_timeframe = gPrimaryExecutionTimeframe;
   if(request_timeframe == "")
      return;
=======
   string request_timeframe = TradingTimeframeToText(InpTradingTimeframe);
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

   string request_id = BuildRequestId("GET_TRADING_EXECUTION_SIGNALS", request_symbol, request_timeframe);
   if(SendTradingExecutionSignalRequest(request_id, request_symbol, request_timeframe))
   {
      gTradingExecutionRequestPending = true;
      gPendingTradingExecutionRequestId = request_id;
      gPendingTradingExecutionSymbol = request_symbol;
      gPendingTradingExecutionTimeframe = request_timeframe;
      gTradingExecutionRequestSentMs = now_ms;
      gLastTradingExecutionPollMs = now_ms;
   }
}

void PollDurationProfile()
{
   ulong now_ms = GetTickCount64();
   if(gDurationProfileRequestPending)
   {
      string response_message = "";
      if(ReceivePendingDurationProfileResponse(DurationProfileRecvTimeoutMs(), response_message))
      {
         HandleDurationProfileResponse(response_message);
         ClearPendingDurationProfileRequest();
      }
      else if((int)(now_ms - gDurationProfileRequestSentMs) > InpCommandRecvTimeoutMs)
      {
         ClearPendingDurationProfileRequest();
      }
      return;
   }

   if((int)(now_ms - gLastDurationProfilePollMs) < DurationProfilePollMs())
      return;

   string request_symbol = BuildRuntimeSymbol();
   string request_timeframe = gDurationProfilePollTimeframes[gDurationProfilePollTimeframeIndex];

   gDurationProfilePollTimeframeIndex++;
   if(gDurationProfilePollTimeframeIndex >= ArraySize(gDurationProfilePollTimeframes))
      gDurationProfilePollTimeframeIndex = 0;

   string request_id = BuildRequestId("GET_DURATION_PROFILE", request_symbol, request_timeframe);
   if(SendDurationProfileRequest(request_id, request_symbol, request_timeframe))
   {
      gDurationProfileRequestPending = true;
      gPendingDurationProfileRequestId = request_id;
      gPendingDurationProfileSymbol = request_symbol;
      gPendingDurationProfileTimeframe = request_timeframe;
      gDurationProfileRequestSentMs = now_ms;
      gLastDurationProfilePollMs = now_ms;
   }
}

void PollLevelVolumeProfile()
{
   ulong now_ms = GetTickCount64();
   if(gLevelVolumeProfileRequestPending)
   {
      string response_message = "";
      if(ReceivePendingLevelVolumeProfileResponse(LevelVolumeProfileRecvTimeoutMs(), response_message))
      {
         HandleLevelVolumeProfileResponse(response_message);
         ClearPendingLevelVolumeProfileRequest();
      }
      else if((int)(now_ms - gLevelVolumeProfileRequestSentMs) > InpCommandRecvTimeoutMs)
      {
         ClearPendingLevelVolumeProfileRequest();
      }
      return;
   }

   if((int)(now_ms - gLastLevelVolumeProfilePollMs) < LevelVolumeProfilePollMs())
      return;

   string request_symbol = BuildRuntimeSymbol();
   string request_timeframe = gDurationProfilePollTimeframes[gLevelVolumeProfilePollTimeframeIndex];

   gLevelVolumeProfilePollTimeframeIndex++;
   if(gLevelVolumeProfilePollTimeframeIndex >= ArraySize(gDurationProfilePollTimeframes))
      gLevelVolumeProfilePollTimeframeIndex = 0;

   string request_id = BuildRequestId("GET_LEVEL_VOLUME_PROFILE", request_symbol, request_timeframe);
   if(SendLevelVolumeProfileRequest(request_id, request_symbol, request_timeframe))
   {
      gLevelVolumeProfileRequestPending = true;
      gPendingLevelVolumeProfileRequestId = request_id;
      gPendingLevelVolumeProfileSymbol = request_symbol;
      gPendingLevelVolumeProfileTimeframe = request_timeframe;
      gLevelVolumeProfileRequestSentMs = now_ms;
      gLastLevelVolumeProfilePollMs = now_ms;
   }
}

void PollVolumeZScoreProfile()
{
   ulong now_ms = GetTickCount64();
   if(gVolumeZScoreProfileRequestPending)
   {
      string response_message = "";
      if(ReceivePendingVolumeZScoreProfileResponse(VolumeZScoreProfileRecvTimeoutMs(), response_message))
      {
         HandleVolumeZScoreProfileResponse(response_message);
         ClearPendingVolumeZScoreProfileRequest();
      }
      else if((int)(now_ms - gVolumeZScoreProfileRequestSentMs) > InpCommandRecvTimeoutMs)
      {
         ClearPendingVolumeZScoreProfileRequest();
      }
      return;
   }

   if((int)(now_ms - gLastVolumeZScoreProfilePollMs) < VolumeZScoreProfilePollMs())
      return;

   string request_symbol = BuildRuntimeSymbol();
   string request_timeframe = gDurationProfilePollTimeframes[gVolumeZScoreProfilePollTimeframeIndex];

   gVolumeZScoreProfilePollTimeframeIndex++;
   if(gVolumeZScoreProfilePollTimeframeIndex >= ArraySize(gDurationProfilePollTimeframes))
      gVolumeZScoreProfilePollTimeframeIndex = 0;

   string request_id = BuildRequestId("GET_VOLUME_ZSCORE_PROFILE", request_symbol, request_timeframe);
   if(SendVolumeZScoreProfileRequest(request_id, request_symbol, request_timeframe))
   {
      gVolumeZScoreProfileRequestPending = true;
      gPendingVolumeZScoreProfileRequestId = request_id;
      gPendingVolumeZScoreProfileSymbol = request_symbol;
      gPendingVolumeZScoreProfileTimeframe = request_timeframe;
      gVolumeZScoreProfileRequestSentMs = now_ms;
      gLastVolumeZScoreProfilePollMs = now_ms;
   }
}

int OnInit()
{
   gExecutionCsvLogger.Configure(InpExecutionDecisionCsvFile);
   gTradingCommandTraceCsvLogger.Configure(InpTradingCommandTraceCsvFile);
   gMetaTraderExecutionClient.Configure(
      &gExecutionCsvLogger,
      InpRiskPercent,
      InpPreferredLeverage,
      InpExecutionMagicNumber,
      InpOrderDeviationPoints,
      InpPostStopReentryDelaySeconds,
      InpReentryStopBufferAtrPeriod,
      InpReentryStopBufferAtrMultiplier
   );
   gTradingStrategyEngine.Configure(&gMetaTraderExecutionClient, &gExecutionCsvLogger, &gTradingCommandTraceCsvLogger);
   gTelegramAccountReporter.Configure(InpTelegramConfigFileName, InpTelegramReportIntervalSeconds);

   if(!gTcpBridgeClient.Connect(InpServerHost, InpServerPort, InpConnectTimeoutMs, InpCommandRecvTimeoutMs))
      return INIT_FAILED;
   if(!SendSystemInit())
      return INIT_FAILED;
   ClearDurationProfileObjects();
   ClearLevelVolumeProfileObjects();
   ClearVolumeZScoreProfileObjects();
   ApplyChartShiftSettingsToAllCharts();
   EventSetMillisecondTimer(RuntimeTimerMs());
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   ClearDurationProfileObjects();
   ClearLevelVolumeProfileObjects();
   ClearVolumeZScoreProfileObjects();
   gTcpBridgeClient.Close();
}

void OnTimer()
{
   PollTradingExecutionSignals();
   MonitorTrackedPositionClosures();
   gTelegramAccountReporter.MaybeSendReport();
}

void OnTick()
{
}
