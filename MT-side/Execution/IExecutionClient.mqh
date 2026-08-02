#ifndef VOLUME_STUDY_EXECUTION_CLIENT_CONTRACT_MQH
#define VOLUME_STUDY_EXECUTION_CLIENT_CONTRACT_MQH

#include "TradingTypes.mqh"

class IExecutionClient
{
public:
   virtual string ClientName()
   {
      return "";
   }

   virtual bool HasOpenPosition(const string symbol, const ENUM_EXECUTION_SIDE side)
   {
      return false;
   }

   virtual bool HasAnyOpenPosition(const string symbol, ENUM_EXECUTION_SIDE &side_out, double &profit_out, string &position_id_out)
   {
      side_out = EXECUTION_SIDE_NONE;
      profit_out = 0.0;
      position_id_out = "";
      return false;
   }

   virtual bool OpenPosition(const STradingExecutionSignal &signal, SExecutionDecisionRecord &record)
   {
      return false;
   }

   virtual bool ClosePosition(const string symbol, const ENUM_EXECUTION_SIDE side, const string timeframe, const string reason, SExecutionDecisionRecord &record)
   {
      return false;
   }

   virtual bool ClosePositionCommand(const STradingExecutionSignal &signal, SExecutionDecisionRecord &record)
   {
      return false;
   }
};

#endif
