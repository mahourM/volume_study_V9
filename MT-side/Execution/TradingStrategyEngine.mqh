#ifndef VOLUME_STUDY_TRADING_STRATEGY_ENGINE_MQH
#define VOLUME_STUDY_TRADING_STRATEGY_ENGINE_MQH

#include "MetaTraderExecutionClient.mqh"
#include "TradingCommandTraceCsvLogger.mqh"

class CTradingStrategyEngine
{
private:
   CMetaTraderExecutionClient *m_execution_client;
   CExecutionCsvLogger *m_logger;
   CTradingCommandTraceCsvLogger *m_trace_logger;

   void AppendStatusUpdate(
      STradingPositionStatusUpdate &updates[],
      const STradingExecutionSignal &command,
      const string status,
      const SExecutionDecisionRecord &record
   )
   {
      int count = ArraySize(updates);
      ArrayResize(updates, count + 1);
      updates[count].client_name = command.client_name;
      updates[count].request_id = command.request_id;
      updates[count].position_id = record.position_id != "" ? record.position_id : command.position_id;
      updates[count].client_position_id = record.client_position_id != "" ? record.client_position_id : command.client_position_id;
      updates[count].client_position_identifier = record.client_position_identifier != "" ? record.client_position_identifier : command.client_position_identifier;
      updates[count].symbol_name = command.symbol_name;
      updates[count].timeframe = command.timeframe;
      updates[count].side = command.side;
      updates[count].status = status;
      updates[count].signal_time_utc_ms = command.signal_time_utc_ms;
      updates[count].cluster_id = command.cluster_id;
      updates[count].profit = 0.0;
      updates[count].entry_price = 0.0;
      updates[count].opened_at_utc_ms = 0;
      updates[count].rejection_reason = record.rejection_reason;

      if(status == "POSITION_OPENED" || status == "POSITION_STILL_OPEN")
      {
         string client_position_id = "";
         string client_position_identifier = "";
         string position_id = "";
         string execution_comment = "";
         double profit = 0.0;
         double entry_price = 0.0;
         long opened_at_utc_ms = 0;
         if(m_execution_client.FindOpenPositionDetails(
            updates[count].position_id,
            command.symbol_name,
            command.side,
            position_id,
            client_position_id,
            client_position_identifier,
            execution_comment,
            profit,
            entry_price,
            opened_at_utc_ms
         ))
         {
            updates[count].position_id = position_id;
            updates[count].client_position_id = client_position_id;
            updates[count].client_position_identifier = client_position_identifier;
            updates[count].profit = profit;
            updates[count].entry_price = entry_price;
            updates[count].opened_at_utc_ms = opened_at_utc_ms;
         }
      }
   }

public:
   CTradingStrategyEngine()
   {
      m_execution_client = NULL;
      m_logger = NULL;
      m_trace_logger = NULL;
   }

   void Configure(
      CMetaTraderExecutionClient *execution_client,
      CExecutionCsvLogger *logger,
      CTradingCommandTraceCsvLogger *trace_logger
   )
   {
      m_execution_client = execution_client;
      m_logger = logger;
      m_trace_logger = trace_logger;
   }

   void Evaluate(
      const string runtime_symbol,
      STradingExecutionSignal &commands[],
      STradingPositionStatusUpdate &status_updates[]
   )
   {
      if(m_execution_client == NULL)
         return;

      for(int i = 0; i < ArraySize(commands); i++)
      {
         STradingExecutionSignal command = commands[i];
         if(m_trace_logger != NULL)
            m_trace_logger.RecordCommand("COMMAND_EVALUATE_RECEIVED", command, i, runtime_symbol, "received", "");
         if(command.symbol_name != runtime_symbol)
         {
            if(m_trace_logger != NULL)
               m_trace_logger.RecordCommand("COMMAND_DROPPED", command, i, runtime_symbol, "rejected", "RUNTIME_SYMBOL_MISMATCH");
            continue;
         }
         if(command.side == EXECUTION_SIDE_NONE)
         {
            if(m_trace_logger != NULL)
               m_trace_logger.RecordCommand("COMMAND_DROPPED", command, i, runtime_symbol, "rejected", "SIDE_NONE");
            continue;
         }

         if(command.command_type == "OPEN")
         {
            if(m_trace_logger != NULL)
               m_trace_logger.RecordCommand("OPEN_ATTEMPT", command, i, runtime_symbol, "attempt", "");
            SExecutionDecisionRecord entry_record;
            bool opened = m_execution_client.OpenPosition(command, entry_record);
            if(m_logger != NULL)
               m_logger.RecordDecision(entry_record);
            AppendStatusUpdate(status_updates, command, opened ? "POSITION_OPENED" : "POSITION_REJECTED", entry_record);
            continue;
         }

         if(command.command_type == "CLOSE")
         {
            if(m_trace_logger != NULL)
               m_trace_logger.RecordCommand("CLOSE_ATTEMPT", command, i, runtime_symbol, "attempt", "");
            SExecutionDecisionRecord exit_record;
            bool closed = m_execution_client.ClosePositionCommand(command, exit_record);
            if(m_logger != NULL)
               m_logger.RecordDecision(exit_record);
            AppendStatusUpdate(status_updates, command, closed ? "POSITION_CLOSED_BY_SIGNAL" : "POSITION_STILL_OPEN", exit_record);
         }
      }
   }
};

#endif
