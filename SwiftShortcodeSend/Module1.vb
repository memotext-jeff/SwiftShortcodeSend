Imports System.Data.SqlClient
Imports System.Configuration
Module Module1

    Sub Main()
        'Make a change
        GetIncomingSMSGateway()
        SendMessages(ConfigurationManager.AppSettings("GetMessagesProc"), "UpdateMessageQue_status")
    End Sub




    Sub SendMessages(ByVal SendFunc As String, ByVal UpdateFunc As String)
        Dim hlxObj As New Halex.hlxfunction
        Dim Params(3) As SqlParameter
        Dim Messages As DataTable
        Dim message As DataRow
        Dim ddate As String, dtime As String

        'get messages to be sent 

        Params(0) = New SqlParameter("@DispatchDate", Date.Now.ToShortDateString)
        Params(1) = New SqlParameter("@dispatchTime", Date.Now.ToShortTimeString)
        Params(2) = New SqlParameter("@Progid", ConfigurationManager.AppSettings("progid"))
        'hlxObj.WriteDebugLog("Before SendMessages- SendFunc", DEBUGLOGFLAG)
        Messages = hlxObj.runStoreProcReader(Params, ConfigurationManager.ConnectionStrings("CSmemotext_com").ConnectionString, SendFunc)
        'hlxObj.WriteDebugLog("After SendMessages- SendFunc", DEBUGLOGFLAG)
        If hlxObj.ErrorCode = 0 Then
            For Each message In Messages.Rows
                '       hlxObj.WriteDebugLog("About to go to Send", DEBUGLOGFLAG)
                Send(message, UpdateFunc)
            Next
        Else
            'EventLog1.WriteEntry(hlxObj.ErrorDescription, EventLogEntryType.Error)
            '  hlxObj.WriteDebugLog(hlxObj.ErrorDescription, DEBUGLOGFLAG)
        End If
        hlxObj = Nothing
        Messages = Nothing
        message = Nothing
    End Sub




    Sub Send(ByVal message As DataRow, ByVal updatefunc As String)

        Dim dictReturn As New Halex.HLXDictionary
        Dim hlxObj As New Halex.hlxfunction


        'hlxObj.WriteDebugLog("In Send routine - Start", DEBUGLOGFLAG)

        If message("TransportType") = "SMS" Then
            '   hlxObj.WriteDebugLog("Go SMS message", DEBUGLOGFLAG)
            'EventLog1.WriteEntry("Going to SMS Send", EventLogEntryType.Information)
            dictReturn = SendSMSGateway(message)
        End If

        If dictReturn Is Nothing Then
            'move on, event log has error, add email at some point
            Exit Sub
        End If
        'If message("email_notify") = "y" And Not dictReturn Is Nothing Then
        '    ' hlxObj.WriteDebugLog("Go Send Email", DEBUGLOGFLAG)
        '    SendEMail(message, dictReturn)
        'End If

        'hlxObj.WriteDebugLog("Execute Update", DEBUGLOGFLAG)
        ExecuteDataUpdate(message, updatefunc, dictReturn)
        'Schedule next message goes here.
        If message("Category") = 3 Then
            SchedNextMessage(message)
        End If

        dictReturn = Nothing
    End Sub


    Sub ExecuteDataUpdate(ByVal message As DataRow, ByVal updateFunc As String,
            ByVal smsReturn As Halex.HLXDictionary)

        Dim hlxFunc As New Halex.hlxfunction
        Dim params(9) As SqlParameter

        params(0) = New SqlParameter("@id", message("ID"))
        params(1) = New SqlParameter("@clientID", message("client_id"))
        params(2) = New SqlParameter("@dsp_date", message("msg_dispatchdate"))
        params(3) = New SqlParameter("@dsp_time", message("msg_dispatchTime"))
        params(4) = New SqlParameter("@mobilenumber", message("msg_mobilenumber"))
        params(5) = New SqlParameter("@ticketid", smsReturn("msgTicketID"))
        params(6) = New SqlParameter("@msgstatus", "d")
        params(7) = New SqlParameter("@errorcode", smsReturn("msgErrorCOde"))
        params(8) = New SqlParameter("@errordesc", smsReturn("msgErrorDesc"))

        hlxFunc.StoredProcParam(params, ConfigurationManager.ConnectionStrings("CSmemotext_com").ConnectionString, updateFunc, Nothing)

        hlxFunc = Nothing

    End Sub

    Sub SchedNextMessage(ByVal message As DataRow)
        Dim hlxFunc As New Halex.hlxfunction
        Dim params(1) As SqlParameter


        params(1) = New SqlParameter("@MSID", message("MSID"))

        hlxFunc.StoredProcParam(params, ConfigurationManager.ConnectionStrings("CSmemotext_com").ConnectionString, "spMessageReschedule", Nothing)

        hlxFunc = Nothing
    End Sub


    Function SendSMSGateway(ByVal Message As DataRow) As Halex.HLXDictionary

        Dim Results As New Halex.HLXDictionary
        Dim ReturnData As New SMSSEND.SMSSendMessageResponse
        Dim IntReturn As String


        Try
            ' Dim sms As New SMSSEND.SendSMS

            Dim sms As New SMSSEND.SendSMSSoapClient

            'ReturnData = sms.SendMessageExtended(Message("msg_mobilenumber"), Message("msg_text"), SMSIDval)
            IntReturn = sms.SendMessageWithReference(Message("msg_mobilenumber"), Message("msg_text"), ConfigurationManager.AppSettings("SMSIDval"), Message("ID"))

            'ventLog1.WriteEntry("After Send Local -> ", EventLogEntryType.Information)
            Results.Add("msgTicketID", "")
            Results.Add("Success", "")
            Results.Add("msgErrorCOde", 0)
            Results.Add("msgErrorDesc", IntReturn)
            Results.Add("msgErrorRes", "")
            sms = Nothing
            Return Results
        Catch ex As Exception
            'ventLog1.WriteEntry("SMS Send-> " & ex.Message & ex.Source.ToString, EventLogEntryType.Error)

            '  sms = Nothing
            Return Nothing
        End Try


    End Function


    Sub GetIncomingSMSGateway()
        'get accounts

        Dim hlxFunc As New Halex.hlxfunction
        Dim sqlText As String
        Dim dt As DataTable
        Dim drow As DataRow
        'EventLog1.WriteEntry("SMS Incoming - In Get", EventLogEntryType.Information)
        Try
            sqlText = "select distinct GatewayAccountKey from Memotext_Client_Programs where right(smsnumber,5) in ('" + ConfigurationManager.AppSettings("Shortcode") + "')"
            dt = hlxFunc.runSQLQuery(sqlText, ConfigurationManager.ConnectionStrings("CSmemotext_com").ConnectionString)

            For Each drow In dt.Rows
                SaveIncomingMessagesGateway(drow)
            Next

            ProcessSMSMessages()
        Catch ex As Exception

            'EventLog1.WriteEntry("GetIncomingSMS--> " & ex.Message, EventLogEntryType.Error)

        End Try

    End Sub

    Sub SaveIncomingMessagesGateway(ByVal drow As DataRow)
        Dim sms As New SMSSEND.SendSMSSoapClient
        Dim Msgs() As SMSSEND.SMSIncomingMessage

        Dim StartMessage As Integer

        Try
            StartMessage = GetStartMessageGateway(drow("GatewayAccountKey"))
            Msgs = sms.GetIncomingMessagesAfterID(drow("GatewayAccountKey"), StartMessage)

            'EventLog1.WriteEntry("SMS Incoming - Msg to save = " & Msgs.GetUpperBound(0), EventLogEntryType.Information)
            For index As Integer = 0 To Msgs.GetUpperBound(0)
                'EventLog1.WriteEntry("SMS Incoming - In retun loop, data returned", EventLogEntryType.Information)
                WriteSMSMessage(Msgs(index), drow("GatewayAccountKey"))
                'EventLog1.WriteEntry("SaveIncomingMessages-->save message", EventLogEntryType.Information)
            Next
        Catch ex As Exception
            'EventLog1.WriteEntry("SaveIncomingMessages--> " & ex.Message, EventLogEntryType.Error)
        End Try
        'StartMessage = GetStartMessage(drow("SMSAccount"))
        'Msgs = sms.GetGrextIncomingMessagesAfterID(drow("SMSAccount"), StartMessage)

        'For index As Integer = 0 To Msgs.GetUpperBound(0)
        '    WriteSMSMessage(Msgs(index))
        'Next


    End Sub

    Function GetStartMessageGateway(ByVal SMSaccount As String) As Integer
        Dim hlxFunc As New Halex.hlxfunction
        Dim sqlText As String
        Dim dt As DataTable

        sqlText = "select isnull(MAX(cast(messagenumber as integer)), 0) as messagenumber from IncomingSMSmessages where AccountKey ='" &
            SMSaccount & "'"
        dt = hlxFunc.runSQLQuery(sqlText, ConfigurationManager.ConnectionStrings("CSmemotext_com").ConnectionString)
        Return dt.Rows(0)("Messagenumber")


    End Function

    Sub WriteSMSMessage(ByVal msg As SMSSEND.SMSIncomingMessage, ByVal AccountKey As String)
        Dim hlxFunc As New Halex.hlxfunction
        Dim params(8) As SqlParameter

        params(1) = New SqlParameter("@MessageNumber", msg.MessageNumber)
        params(2) = New SqlParameter("@OutgoingMessageID", msg.OutgoingMessageID)
        params(3) = New SqlParameter("@AccountKey", AccountKey)
        params(4) = New SqlParameter("@Reference", msg.Reference)
        params(5) = New SqlParameter("@PhoneNumber", msg.PhoneNumber)
        params(6) = New SqlParameter("@Message", msg.Message)
        params(7) = New SqlParameter("@ReceivedDate", msg.ReceivedDate)
        params(8) = New SqlParameter("@Processed", 0)

        hlxFunc.StoredProcParam(params, ConfigurationManager.ConnectionStrings("CSmemotext_com").ConnectionString, "WriteIncSMS", Nothing)
    End Sub

    Sub ProcessSMSMessages()
        Dim hlxFunc As New Halex.hlxfunction


        hlxFunc.StoredProcParam(Nothing, ConfigurationManager.ConnectionStrings("CSmemotext_com").ConnectionString, "ProcessSMSResponse_Gateway2", Nothing)

    End Sub
End Module
