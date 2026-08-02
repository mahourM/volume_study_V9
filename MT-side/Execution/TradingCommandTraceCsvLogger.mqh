#ifndef VOLUME_STUDY_TRADING_COMMAND_TRACE_CSV_LOGGER_MQH
#define VOLUME_STUDY_TRADING_COMMAND_TRACE_CSV_LOGGER_MQH

#include "TradingTypes.mqh"

class CTradingCommandTraceCsvLogger
{
private:
   string m_file_name;

   void WriteHeaderIfNeeded(const int handle)
   {
      if(FileSize(handle) > 0)
         return;

      FileWrite(
         handle,
         "broker_time",
         "stage",
         "request_id",
         "pending_request_id",
         "message_type",
         "response_symbol",
         "pending_symbol",
         "response_primary_timeframe",
         "pending_primary_timeframe",
         "command_index",
         "command_type",
<<<<<<< HEAD
         "action",
         "target_entry_open_time_utc_ms",
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
         "symbol_name",
         "runtime_symbol",
         "timeframe",
         "side",
         "position_id",
         "command_count",
         "parsed_count",
         "result",
         "reason"
      );
   }

public:
   void Configure(const string file_name)
   {
      m_file_name = file_name;
      if(m_file_name == "")
         m_file_name = "trading_command_trace.csv";
   }

   bool Record(
      const string stage,
      const string request_id,
      const string pending_request_id,
      const string message_type,
      const string response_symbol,
      const string pending_symbol,
      const string response_primary_timeframe,
      const string pending_primary_timeframe,
      const int command_index,
      const string command_type,
      const string symbol_name,
      const string runtime_symbol,
      const string timeframe,
      const string side,
      const string position_id,
      const int command_count,
      const int parsed_count,
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
         TimeToString(TimeCurrent(), TIME_DATE | TIME_SECONDS),
         stage,
         request_id,
         pending_request_id,
         message_type,
         response_symbol,
         pending_symbol,
         response_primary_timeframe,
         pending_primary_timeframe,
         (string)command_index,
         command_type,
<<<<<<< HEAD
         action,
         (string)target_entry_open_time_utc_ms,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
         symbol_name,
         runtime_symbol,
         timeframe,
         side,
         position_id,
         (string)command_count,
         (string)parsed_count,
         result,
         reason
      );
      FileClose(handle);
      return true;
   }

   bool RecordCommand(
      const string stage,
      const STradingExecutionSignal &command,
      const int command_index,
      const string runtime_symbol,
      const string result,
      const string reason
   )
   {
      return Record(
         stage,
         command.request_id,
         "",
         "",
         "",
         "",
         "",
         "",
         command_index,
         command.command_type,
         command.symbol_name,
         runtime_symbol,
         command.timeframe,
         ExecutionSideToText(command.side),
         command.position_id,
         0,
         0,
         result,
<<<<<<< HEAD
         reason,
         command.action,
         command.target_entry_open_time_utc_ms
=======
         reason
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
      );
   }
};

#endif
