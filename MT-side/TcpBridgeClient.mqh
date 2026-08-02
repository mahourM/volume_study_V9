#import "libmt5_tcp_bridge.dll"
int  tcp_connect(const uchar &host[], int port, int timeout_ms);
int  tcp_send(int handle, const uchar &data[], int len);
int  tcp_recv_line(int handle, uchar &out[], int out_cap, int timeout_ms);
void tcp_close(int handle);
#import

class CTcpBridgeClient
{
private:
   int  m_handle;
   int  m_recv_timeout_ms;
   bool m_is_connected;

   void StringToUtf8Bytes(const string source_text, uchar &output_bytes[])
   {
      StringToCharArray(source_text, output_bytes, 0, WHOLE_ARRAY, CP_UTF8);
   }

public:
   CTcpBridgeClient()
   {
      m_handle = -1;
      m_recv_timeout_ms = 500;
      m_is_connected = false;
   }

   bool Connect(const string host, const int port, const int connect_timeout_ms, const int recv_timeout_ms)
   {
      uchar host_bytes[];
      StringToUtf8Bytes(host, host_bytes);

      m_handle = tcp_connect(host_bytes, port, connect_timeout_ms);
      m_recv_timeout_ms = recv_timeout_ms;
      m_is_connected = (m_handle >= 0);
      return m_is_connected;
   }

   bool SendLine(const string message_text)
   {
      if(!m_is_connected || m_handle < 0)
         return false;

      string message_with_newline = message_text + "\n";
      uchar message_bytes[];
      StringToUtf8Bytes(message_with_newline, message_bytes);

      int bytes_to_send = ArraySize(message_bytes) - 1;
      int sent_len = tcp_send(m_handle, message_bytes, bytes_to_send);
      return (sent_len == bytes_to_send);
   }

   bool ReceiveLine(string &message_text)
   {
      return ReceiveLineWithTimeout(m_recv_timeout_ms, message_text);
   }

   bool ReceiveLineWithTimeout(const int timeout_ms, string &message_text)
   {
      message_text = "";
   
      if(!m_is_connected)
         return false;
      if(m_handle < 0)
         return false;
   
      uchar response_bytes[];
      ArrayResize(response_bytes, 1024 * 1024);
   
      int recv_len = tcp_recv_line(m_handle, response_bytes, ArraySize(response_bytes), timeout_ms);
      if(recv_len <= 0)
         return false;
   
      message_text = CharArrayToString(response_bytes, 0, recv_len, CP_UTF8);
      return (message_text != "");
   }

   void Close()
   {
      if(m_handle >= 0)
         tcp_close(m_handle);

      m_handle = -1;
      m_is_connected = false;
   }
};
