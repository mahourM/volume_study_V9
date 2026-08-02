#ifndef VOLUME_STUDY_TELEGRAM_ACCOUNT_REPORTER_MQH
#define VOLUME_STUDY_TELEGRAM_ACCOUNT_REPORTER_MQH

class CTelegramAccountReporter
{
private:
   int m_interval_seconds;
   int m_retry_delay_seconds;
   datetime m_last_report_time;
   datetime m_next_retry_time;
   datetime m_last_system_error_log_time;
   string m_bot_token;
   string m_chat_id;

   void LogSystemError(const string reason, const int status_code, const int mt5_error_code)
   {
      datetime now = TimeCurrent();
      if(m_last_system_error_log_time > 0 && (now - m_last_system_error_log_time) < 60)
         return;
      m_last_system_error_log_time = now;
      PrintFormat(
         "TELEGRAM_ACCOUNT_REPORT_ERROR | reason=%s | status_code=%d | mt5_error_code=%d",
         reason,
         status_code,
         mt5_error_code
      );
   }

   bool LoadConfig()
   {
      if(m_bot_token != "" && m_chat_id != "")
         return true;

      m_bot_token = "7952172017:AAFnUlRNqltR_vLf2hl_ViGVRm0B-8impsE";
      m_chat_id = "104544751";
      if(m_bot_token == "" || m_chat_id == "")
      {
         LogSystemError("TELEGRAM_CREDENTIALS_NOT_SET", 0, 0);
         return false;
      }
      return (m_bot_token != "" && m_chat_id != "");
   }

   void CalculateEdge(double &edge, double &edge_percent)
   {
      int profit_count = 0;
      int loss_count = 0;
      double profit_sum = 0.0;
      double loss_sum = 0.0;

      if(HistorySelect(0, TimeCurrent()))
      {
         for(int i = 0; i < HistoryDealsTotal(); i++)
         {
            ulong ticket = HistoryDealGetTicket(i);
            if(ticket == 0)
               continue;
            long entry = HistoryDealGetInteger(ticket, DEAL_ENTRY);
            if(entry != DEAL_ENTRY_OUT && entry != DEAL_ENTRY_INOUT && entry != DEAL_ENTRY_OUT_BY)
               continue;
            double profit = HistoryDealGetDouble(ticket, DEAL_PROFIT);
            if(profit > 0.0)
            {
               profit_count++;
               profit_sum += profit;
            }
            else if(profit < 0.0)
            {
               loss_count++;
               loss_sum += MathAbs(profit);
            }
         }
      }

      int total_count = profit_count + loss_count;
      if(total_count <= 0)
      {
         edge = 0.0;
         edge_percent = 0.0;
         return;
      }

      double average_profit = (profit_count > 0) ? profit_sum / profit_count : 0.0;
      double average_loss = (loss_count > 0) ? loss_sum / loss_count : 0.0;
      edge = (((double)profit_count / (double)total_count) * average_profit)
           - (((double)loss_count / (double)total_count) * average_loss);

      double balance = AccountInfoDouble(ACCOUNT_BALANCE);
      edge_percent = (balance != 0.0) ? (edge / balance) * 100.0 : 0.0;
   }

   bool SendMessage(const string message_text)
   {
      if(!LoadConfig())
         return false;

      string url = "https://api.telegram.org/bot" + m_bot_token + "/sendMessage";
      string body = "chat_id=" + m_chat_id + "&text=" + UrlEncode(message_text);
      uchar payload[];
      StringToCharArray(body, payload, 0, WHOLE_ARRAY, CP_UTF8);
      ArrayResize(payload, ArraySize(payload) - 1);

      uchar result[];
      string result_headers = "";
      string headers = "Content-Type: application/x-www-form-urlencoded\r\n";
      ResetLastError();
      int status_code = WebRequest("POST", url, headers, 10000, payload, result, result_headers);
      if(status_code < 200 || status_code >= 300)
      {
         LogSystemError("TELEGRAM_WEBREQUEST_FAILED", status_code, GetLastError());
         return false;
      }
      return true;
   }

   string UrlEncode(const string source)
   {
      string result = "";
      uchar bytes[];
      StringToCharArray(source, bytes, 0, WHOLE_ARRAY, CP_UTF8);
      for(int i = 0; i < ArraySize(bytes) - 1; i++)
      {
         uchar ch = bytes[i];
         if((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '~')
            result += CharToString(ch);
         else if(ch == ' ')
            result += "+";
         else
            result += StringFormat("%%%02X", ch);
      }
      return result;
   }

public:
   CTelegramAccountReporter()
   {
      m_interval_seconds = 10800;
      m_retry_delay_seconds = 60;
      m_last_report_time = 0;
      m_next_retry_time = 0;
      m_last_system_error_log_time = 0;
      m_bot_token = "";
      m_chat_id = "";
   }

   void Configure(const string config_file_name, const int interval_seconds)
   {
      string ignored_config_file_name = config_file_name;
      if(ignored_config_file_name == "")
      {
      }
      m_interval_seconds = interval_seconds;
      if(m_interval_seconds <= 0)
         m_interval_seconds = 10800;
      m_bot_token = "";
      m_chat_id = "";
      m_next_retry_time = 0;
   }

   void MaybeSendReport()
   {
      datetime now = TimeCurrent();
      if(m_next_retry_time > 0 && now < m_next_retry_time)
         return;
      if(m_last_report_time > 0 && (now - m_last_report_time) < m_interval_seconds)
         return;

      double balance = AccountInfoDouble(ACCOUNT_BALANCE);
      double equity = AccountInfoDouble(ACCOUNT_EQUITY);
      double edge = 0.0;
      double edge_percent = 0.0;
      CalculateEdge(edge, edge_percent);

      string message = StringFormat(
         "Account Report\n\nBalance: %.2f\nEquity: %.2f\nEdge: %.2f\nEdge %%: %.2f",
         balance,
         equity,
         edge,
         edge_percent
      );

      if(SendMessage(message))
      {
         m_last_report_time = now;
         m_next_retry_time = 0;
      }
      else
      {
         m_next_retry_time = now + m_retry_delay_seconds;
      }
   }
};

#endif
