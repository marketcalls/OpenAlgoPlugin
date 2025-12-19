// Plugin.cpp - Complete implementation with Quotes and Historical data
#include "stdafx.h"
#include "resource.h"  // Include resource definitions
#include "OpenAlgoGlobals.h"
#include "Plugin.h"
#include "Plugin_Legacy.h"
#include "OpenAlgoConfigDlg.h"
#include <math.h>
#include <time.h>
#include <stdlib.h>  // For qsort

// Plugin identification
#define PLUGIN_NAME "OpenAlgo Data Plugin"
#define VENDOR_NAME "OpenAlgo Community"
#define PLUGIN_VERSION 10003
#define PLUGIN_ID PIDCODE('T', 'E', 'S', 'T')  // Unique 4-char code
#define THIS_PLUGIN_TYPE PLUGIN_TYPE_DATA
#define AGENT_NAME PLUGIN_NAME

// Timer IDs
#define TIMER_INIT 198
#define TIMER_REFRESH 199
#define TIMER_WEBSOCKET 200  // High-frequency timer for WebSocket data processing
#define RETRY_COUNT 8

////////////////////////////////////////
// Plugin Info Structure
////////////////////////////////////////
static struct PluginInfo oPluginInfo =
{
	sizeof(struct PluginInfo),
	THIS_PLUGIN_TYPE,
	PLUGIN_VERSION,
	PLUGIN_ID,
	PLUGIN_NAME,
	VENDOR_NAME,
	0,
	530000
};

///////////////////////////////
// Global Variables
///////////////////////////////
HWND g_hAmiBrokerWnd = NULL;
int g_nPortNumber = 5000;
int g_nRefreshInterval = 5;
int g_nTimeShift = 0;
CString g_oServer = _T("127.0.0.1");
CString g_oApiKey = _T("");  // API Key for authentication
CString g_oWebSocketUrl = _T("ws://127.0.0.1:8765");  // WebSocket URL
int g_nStatus = STATUS_WAIT;

// Backfill request tracking
int g_nBackfillDays = 0;        // Number of days to backfill (0 = use default logic)
int g_nBackfillPeriodicity = 0; // 60 for 1-minute, 86400 for daily
BOOL g_bBackfillRequested = FALSE;

// Local static variables
static int g_nRetryCount = RETRY_COUNT;
static struct RecentInfo* g_aInfos = NULL;
static int RecentInfoSize = 0;
static BOOL g_bPluginInitialized = FALSE;

// WebSocket connection management
static SOCKET g_websocket = INVALID_SOCKET;
static BOOL g_bWebSocketConnected = FALSE;
static BOOL g_bWebSocketAuthenticated = FALSE;
static BOOL g_bWebSocketConnecting = FALSE;
static DWORD g_dwLastConnectionAttempt = 0;
static CMap<CString, LPCTSTR, BOOL, BOOL> g_SubscribedSymbols;
static CRITICAL_SECTION g_WebSocketCriticalSection;
static BOOL g_bCriticalSectionInitialized = FALSE;

// Cache for recent quotes
struct QuoteCache {
	CString symbol;
	CString exchange;
	float ltp;
	float open;
	float high;
	float low;
	float close;
	float volume;
	float oi;
	DWORD lastUpdate;
	
	// Constructor to initialize all values
	QuoteCache() : ltp(0.0f), open(0.0f), high(0.0f), low(0.0f), 
	               close(0.0f), volume(0.0f), oi(0.0f), lastUpdate(0) {
	}
};

static CMap<CString, LPCTSTR, QuoteCache, QuoteCache&> g_QuoteCache;

typedef CArray< struct Quotation, struct Quotation > CQuoteArray;

//////////////////////////////////////////////////////////
// REAL-TIME CANDLE BUILDING STRUCTURES
//////////////////////////////////////////////////////////

// Real-time configuration (non-static so they can be accessed from OpenAlgoConfigDlg)
BOOL g_bRealTimeCandlesEnabled = TRUE;  // Default: enabled
int g_nBackfillIntervalMs = 5000;       // HTTP backfill every 5 seconds

// HTTP response caching (performance optimization)
// Cache HTTP responses to avoid calling HTTP API on every GetQuotesEx() call
CMapStringToPtr g_HttpResponseCache;  // Maps "SYMBOL-PERIODICITY" → last HTTP call time (DWORD*)
CRITICAL_SECTION g_HttpCacheCriticalSection;
const DWORD HTTP_CACHE_LIFETIME_MS = 60000;  // Cache HTTP responses for 60 seconds
static BOOL g_bHttpCacheCriticalSectionInitialized = FALSE;

// BarBuilder: Per-symbol tick-to-bar aggregation state
struct BarBuilder {
	CString symbol;
	CString exchange;
	int periodicity;  // 60 for 1-minute (only 1-minute supported initially)

	// Current bar being built from ticks
	struct Quotation currentBar;
	BOOL bBarStarted;
	time_t barStartTime;

	// Tick accumulation
	float volumeAccumulator;  // Sum of last_trade_quantity
	int tickCount;

	// Historical bars storage
	CArray<struct Quotation, struct Quotation> bars;  // Up to 10,000 bars
	int maxBars;

	// Timestamps for backfill management
	DWORD lastTickTime;      // Last tick received
	DWORD lastBackfillTime;  // Last HTTP backfill

	// State flags
	BOOL bBackfillMerged;
	BOOL bFirstTickReceived;

	// Constructor
	BarBuilder() : periodicity(60), bBarStarted(FALSE), barStartTime(0),
	               volumeAccumulator(0.0f), tickCount(0), maxBars(10000),
	               lastTickTime(0), lastBackfillTime(0),
	               bBackfillMerged(FALSE), bFirstTickReceived(FALSE) {
		memset(&currentBar, 0, sizeof(struct Quotation));
	}
};

// Global cache of bar builders (one per symbol)
static CMap<CString, LPCTSTR, BarBuilder*, BarBuilder*> g_BarBuilders;
static CRITICAL_SECTION g_BarBuilderCriticalSection;
static BOOL g_bBarBuilderCriticalSectionInitialized = FALSE;

// Forward declarations
VOID CALLBACK OnTimerProc(HWND, UINT, UINT_PTR, DWORD);
void SetupRetry(void);
BOOL TestOpenAlgoConnection(void);
BOOL GetOpenAlgoQuote(LPCTSTR pszTicker, QuoteCache& quote);
int GetOpenAlgoHistory(LPCTSTR pszTicker, int nPeriodicity, int nLastValid, int nSize, struct Quotation* pQuotes);
CString GetExchangeFromTicker(LPCTSTR pszTicker);
CString GetIntervalString(int nPeriodicity);
void ConvertUnixToPackedDate(time_t unixTime, union AmiDate* pAmiDate);

// WebSocket functions
BOOL InitializeWebSocket(void);
void CleanupWebSocket(void);
BOOL ConnectWebSocket(void);
BOOL AuthenticateWebSocket(void);
BOOL SendWebSocketFrame(const CString& message);
CString DecodeWebSocketFrame(const char* buffer, int length);
BOOL SubscribeToSymbol(LPCTSTR pszTicker);
BOOL UnsubscribeFromSymbol(LPCTSTR pszTicker);
BOOL ProcessWebSocketData(void);
void GenerateWebSocketMaskKey(unsigned char* maskKey);
void SubscribePendingSymbols(void);

// Real-time candle building functions
BOOL ProcessTick(const CString& symbol, const CString& exchange, float ltp, float lastTradeQty, time_t timestamp);
time_t ParseISO8601Timestamp(const CString& isoTimestamp);
BarBuilder* GetOrCreateBarBuilder(const CString& ticker);
void CleanupBarBuilders(void);

// Helper function for mixed EOD/Intraday data
int FindLastBarOfMatchingType(int nPeriodicity, int nLastValid, struct Quotation* pQuotes);

// Helper function to compare two quotations for sorting by timestamp
int CompareQuotations(const void* a, const void* b);

///////////////////////////////
// Helper Functions
///////////////////////////////

// Compare two quotations for sorting by timestamp (oldest to newest)
// Used by qsort() to ensure quotes array is in chronological order
// This is CRITICAL for AmiBroker to display charts correctly
int CompareQuotations(const void* a, const void* b)
{
	const struct Quotation* qa = (const struct Quotation*)a;
	const struct Quotation* qb = (const struct Quotation*)b;

	// Compare 64-bit DateTime.Date field directly
	// This handles both EOD and Intraday data correctly
	if (qa->DateTime.Date < qb->DateTime.Date)
		return -1;
	else if (qa->DateTime.Date > qb->DateTime.Date)
		return 1;
	else
		return 0;
}

// Find last bar in array that matches the requested periodicity type
// This is CRITICAL for Mixed EOD/Intraday support (AllowMixedEODIntra = TRUE)
//
// When mixed data is enabled, pQuotes array contains BOTH:
// - Daily bars (Hour=31, Minute=63)
// - Intraday bars (Hour=0-23)
//
// We must find the last bar of the CORRECT type to calculate gaps properly
int FindLastBarOfMatchingType(int nPeriodicity, int nLastValid, struct Quotation* pQuotes)
{
	if (nLastValid < 0 || pQuotes == NULL)
		return -1;  // No data at all

	if (nPeriodicity == 86400)  // Looking for Daily data
	{
		// Scan backwards to find last Daily bar (Hour=31, Minute=63)
		for (int i = nLastValid; i >= 0; i--)
		{
			if (pQuotes[i].DateTime.PackDate.Hour == DATE_EOD_HOURS &&
				pQuotes[i].DateTime.PackDate.Minute == DATE_EOD_MINUTES)
			{
				return i;  // Found last Daily bar
			}
		}
		return -1;  // No Daily bars found in array
	}
	else if (nPeriodicity == 60)  // Looking for 1-minute (or other intraday) data
	{
		// Scan backwards to find last Intraday bar (Hour < 31)
		for (int i = nLastValid; i >= 0; i--)
		{
			if (pQuotes[i].DateTime.PackDate.Hour < DATE_EOD_HOURS)
			{
				return i;  // Found last Intraday bar
			}
		}
		return -1;  // No Intraday bars found in array
	}
	else
	{
		// Unknown periodicity - use last bar overall
		return nLastValid;
	}
}

CString BuildOpenAlgoURL(const CString& server, int port, const CString& endpoint)
{
	CString result;
	result.Format(_T("http://%s:%d%s"), (LPCTSTR)server, port, (LPCTSTR)endpoint);
	return result;
}

// Extract exchange from ticker format (e.g., "RELIANCE-NSE" -> "NSE")
CString GetExchangeFromTicker(LPCTSTR pszTicker)
{
	CString ticker(pszTicker);
	int dashPos = ticker.ReverseFind(_T('-'));
	if (dashPos != -1)
	{
		return ticker.Mid(dashPos + 1);
	}
	
	// Default to NSE if no exchange suffix found
	return _T("NSE");
}

// Get clean symbol without exchange suffix
CString GetCleanSymbol(LPCTSTR pszTicker)
{
	CString ticker(pszTicker);
	int dashPos = ticker.ReverseFind(_T('-'));
	if (dashPos != -1)
	{
		return ticker.Left(dashPos);
	}
	return ticker;
}

// Convert periodicity to OpenAlgo interval string
// Currently supporting only 1m and D (daily) intervals
CString GetIntervalString(int nPeriodicity)
{
	// Only support 1-minute and Daily for now
	if (nPeriodicity == 60)  // 1 minute in seconds
		return _T("1m");
	else if (nPeriodicity == 86400) // Daily in seconds (24*60*60)
		return _T("D");  // Daily
	else
		return _T("D");  // Default to daily for all other timeframes
}

// Convert Unix timestamp to AmiBroker date format
// Works for all market types including 24x7 markets
void ConvertUnixToPackedDate(time_t unixTime, union AmiDate* pAmiDate)
{
	struct tm* timeinfo = localtime(&unixTime);

	pAmiDate->PackDate.Year = timeinfo->tm_year + 1900;
	pAmiDate->PackDate.Month = timeinfo->tm_mon + 1;
	pAmiDate->PackDate.Day = timeinfo->tm_mday;
	pAmiDate->PackDate.Hour = timeinfo->tm_hour;
	pAmiDate->PackDate.Minute = timeinfo->tm_min;
	pAmiDate->PackDate.Second = timeinfo->tm_sec;
	pAmiDate->PackDate.MilliSec = 0;
	pAmiDate->PackDate.MicroSec = 0;
	pAmiDate->PackDate.Reserved = 0;
	pAmiDate->PackDate.IsFuturePad = 0;
}

// Fetch real-time quote from OpenAlgo
// WARNING: This is ONLY for Level 1 quotes in Real-time Quote Window
// NEVER use this data for creating OHLC bars or historical charts
BOOL GetOpenAlgoQuote(LPCTSTR pszTicker, QuoteCache& quote)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	if (g_oApiKey.IsEmpty())
		return FALSE;

	BOOL bSuccess = FALSE;

	try
	{
		CString oURL = BuildOpenAlgoURL(g_oServer, g_nPortNumber, _T("/api/v1/quotes"));

		// Prepare POST data
		CString symbol = GetCleanSymbol(pszTicker);
		CString exchange = GetExchangeFromTicker(pszTicker);

		CString oPostData;
		oPostData.Format(_T("{\"apikey\":\"%s\",\"symbol\":\"%s\",\"exchange\":\"%s\"}"),
			(LPCTSTR)g_oApiKey, (LPCTSTR)symbol, (LPCTSTR)exchange);

		CInternetSession oSession(AGENT_NAME, 1, INTERNET_OPEN_TYPE_DIRECT, NULL, NULL,
			INTERNET_FLAG_DONT_CACHE);
		oSession.SetOption(INTERNET_OPTION_CONNECT_TIMEOUT, 3000);
		oSession.SetOption(INTERNET_OPTION_RECEIVE_TIMEOUT, 3000);

		CHttpConnection* pConnection = NULL;
		CHttpFile* pFile = NULL;

		// Parse server
		INTERNET_PORT nPort = (INTERNET_PORT)g_nPortNumber;
		CString oServer = g_oServer;
		oServer.Replace(_T("http://"), _T(""));
		oServer.Replace(_T("https://"), _T(""));

		pConnection = oSession.GetHttpConnection(oServer, nPort);

		if (pConnection)
		{
			pFile = pConnection->OpenRequest(
				CHttpConnection::HTTP_VERB_POST,
				_T("/api/v1/quotes"),
				NULL, 1, NULL, NULL,
				INTERNET_FLAG_RELOAD | INTERNET_FLAG_DONT_CACHE);

			if (pFile)
			{
				CString oHeaders = _T("Content-Type: application/json\r\n");
				CStringA oPostDataA(oPostData);

				if (pFile->SendRequest(oHeaders, (LPVOID)(LPCSTR)oPostDataA, oPostDataA.GetLength()))
				{
					DWORD dwStatusCode = 0;
					pFile->QueryInfoStatusCode(dwStatusCode);

					if (dwStatusCode == 200)
					{
						CString oResponse;
						CString oLine;
						while (pFile->ReadString(oLine))
						{
							oResponse += oLine;
						}

						// Parse JSON response (simple parsing)
						if (oResponse.Find(_T("\"status\":\"success\"")) >= 0)
						{
							// Extract values using simple string parsing
							int pos;

							// Parse LTP
							pos = oResponse.Find(_T("\"ltp\":"));
							if (pos >= 0)
							{
								pos += 6;
								int endPos = oResponse.Find(_T(","), pos);
								if (endPos < 0) endPos = oResponse.Find(_T("}"), pos);
								CString val = oResponse.Mid(pos, endPos - pos);
								quote.ltp = (float)_tstof(val);
							}

							// Parse Open
							pos = oResponse.Find(_T("\"open\":"));
							if (pos >= 0)
							{
								pos += 7;
								int endPos = oResponse.Find(_T(","), pos);
								if (endPos < 0) endPos = oResponse.Find(_T("}"), pos);
								CString val = oResponse.Mid(pos, endPos - pos);
								quote.open = (float)_tstof(val);
							}

							// Parse High
							pos = oResponse.Find(_T("\"high\":"));
							if (pos >= 0)
							{
								pos += 7;
								int endPos = oResponse.Find(_T(","), pos);
								if (endPos < 0) endPos = oResponse.Find(_T("}"), pos);
								CString val = oResponse.Mid(pos, endPos - pos);
								quote.high = (float)_tstof(val);
							}

							// Parse Low
							pos = oResponse.Find(_T("\"low\":"));
							if (pos >= 0)
							{
								pos += 6;
								int endPos = oResponse.Find(_T(","), pos);
								if (endPos < 0) endPos = oResponse.Find(_T("}"), pos);
								CString val = oResponse.Mid(pos, endPos - pos);
								quote.low = (float)_tstof(val);
							}

							// Parse Volume
							pos = oResponse.Find(_T("\"volume\":"));
							if (pos >= 0)
							{
								pos += 9;
								int endPos = oResponse.Find(_T(","), pos);
								if (endPos < 0) endPos = oResponse.Find(_T("}"), pos);
								CString val = oResponse.Mid(pos, endPos - pos);
								quote.volume = (float)_tstof(val);
							}

							// Parse OI
							pos = oResponse.Find(_T("\"oi\":"));
							if (pos >= 0)
							{
								pos += 5;
								int endPos = oResponse.Find(_T(","), pos);
								if (endPos < 0) endPos = oResponse.Find(_T("}"), pos);
								CString val = oResponse.Mid(pos, endPos - pos);
								quote.oi = (float)_tstof(val);
							}

							// Parse Previous Close
							pos = oResponse.Find(_T("\"prev_close\":"));
							if (pos >= 0)
							{
								pos += 13;
								int endPos = oResponse.Find(_T(","), pos);
								if (endPos < 0) endPos = oResponse.Find(_T("}"), pos);
								CString val = oResponse.Mid(pos, endPos - pos);
								quote.close = (float)_tstof(val);
							}

							quote.symbol = symbol;
							quote.exchange = exchange;
							quote.lastUpdate = (DWORD)GetTickCount64();

							bSuccess = TRUE;
						}
					}
				}

				pFile->Close();
				delete pFile;
			}

			pConnection->Close();
			delete pConnection;
		}

		oSession.Close();
	}
	catch (CInternetException* e)
	{
		e->Delete();
	}

	return bSuccess;
}

// Fetch historical data from OpenAlgo with intelligent backfill strategy
// COMPLETELY EXCHANGE-AGNOSTIC - Works with ANY exchange and ANY trading hours
//
// OPTIMAL GAP-FREE BACKFILL STRATEGY (DATE-BASED):
// - First load: 30 days (1m) or 10 years (daily) of historical data
// - Subsequent refreshes: Smart gap detection from last bar's date
// - Automatically fills data gaps (user absences, network outages, etc.)
// - Safety limits: Max 30 days for 1m, max 730 days (2 years) for daily
// - Zero data holes within safety limits
//
// HOW IT WORKS:
// 1. Extract last bar's DATE (not time) from existing data
// 2. Calculate gap in days between last bar and today
// 3. Apply safety limits to prevent server overload
// 4. Request date range from OpenAlgo API (DATE-only, no time)
// 5. API returns ALL bars for each date in range
// 6. Duplicate detection handles overlapping bars
//
// EXCHANGE SUPPORT:
// - NSE: Regular + evening sessions + special Sunday sessions
// - MCX: Overnight sessions + extended hours
// - Crypto: 24x7 including weekends
// - Any future sessions that exchanges may introduce
//
// Currently supports 1m and D (daily) intervals
int GetOpenAlgoHistory(LPCTSTR pszTicker, int nPeriodicity, int nLastValid, int nSize, struct Quotation* pQuotes)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	if (g_oApiKey.IsEmpty())
		return nLastValid + 1;

	try
	{
		CString oURL = BuildOpenAlgoURL(g_oServer, g_nPortNumber, _T("/api/v1/history"));

		// Prepare POST data
		CString symbol = GetCleanSymbol(pszTicker);
		CString exchange = GetExchangeFromTicker(pszTicker);
		CString interval = GetIntervalString(nPeriodicity);

		// Get current time and today's date (at midnight)
		CTime currentTime = CTime::GetCurrentTime();
		CTime todayDate = CTime(currentTime.GetYear(), currentTime.GetMonth(), currentTime.GetDay(), 0, 0, 0);

		CTime startTime;
		CTime endTime = todayDate;  // Request up to today for both Daily and Intraday

		// Check if manual backfill was requested - override normal logic
		if (g_bBackfillRequested && g_nBackfillPeriodicity == nPeriodicity && g_nBackfillDays > 0)
		{
			// Manual backfill - fetch full requested period regardless of existing data
			startTime = todayDate - CTimeSpan((LONG)g_nBackfillDays, 0, 0, 0);
			g_bBackfillRequested = FALSE;  // Clear the request
			goto skip_gap_detection;
		}

		// SMART GAP-FREE BACKFILL LOGIC WITH MIXED EOD/INTRADAY SUPPORT
		if (nLastValid >= 0 && pQuotes != NULL)
		{
			// ============================================================
			// EXISTING DATA FOUND - Smart Gap Detection
			// ============================================================

			// CRITICAL: Find last bar of MATCHING TYPE for mixed data support
			// When AllowMixedEODIntra = TRUE, array contains both Daily and Intraday bars
			// We must find the last bar that matches our requested periodicity!
			int lastMatchingBarIndex = FindLastBarOfMatchingType(nPeriodicity, nLastValid, pQuotes);

			if (lastMatchingBarIndex < 0)
			{
				// No bars of matching type found → Treat as initial load
				// Example: Requesting Daily data but only 1-minute bars exist
				if (nPeriodicity == 60)
					startTime = todayDate - CTimeSpan(30, 0, 0, 0);
				else
					startTime = todayDate - CTimeSpan(3650, 0, 0, 0);

				goto skip_gap_detection;
			}

			// Extract last matching bar's DATE (ignore time component)
			int lastBarYear = pQuotes[lastMatchingBarIndex].DateTime.PackDate.Year;
			int lastBarMonth = pQuotes[lastMatchingBarIndex].DateTime.PackDate.Month;
			int lastBarDay = pQuotes[lastMatchingBarIndex].DateTime.PackDate.Day;

			// Create CTime for last bar's date (at midnight)
			CTime lastBarDate;
			try
			{
				lastBarDate = CTime(lastBarYear, lastBarMonth, lastBarDay, 0, 0, 0);
			}
			catch (...)
			{
				// Invalid date in last bar (corrupted data)
				// Fall back to initial load
				if (nPeriodicity == 60)
					startTime = todayDate - CTimeSpan(30, 0, 0, 0);
				else
					startTime = todayDate - CTimeSpan(3650, 0, 0, 0);

				goto skip_gap_detection;
			}

			// VALIDATE: Check if last bar is in the future (corrupted data)
			if (lastBarDate > todayDate)
			{
				// Last bar is in future - corrupted data detected
				// Fall back to initial load
				if (nPeriodicity == 60)
					startTime = todayDate - CTimeSpan(30, 0, 0, 0);
				else
					startTime = todayDate - CTimeSpan(3650, 0, 0, 0);

				goto skip_gap_detection;
			}

			// Calculate gap in days
			CTimeSpan gap = todayDate - lastBarDate;
			int gapDays = (int)gap.GetDays();

			// DEBUG: Log backfill info for diagnosis
			//CString debugMsg;
			//debugMsg.Format(_T("BACKFILL - Symbol: %s, GapDays: %d, LastBar: %s, Today: %s, Periodicity: %d"),
			//	pszTicker, gapDays,
			//	lastBarDate.Format(_T("%Y-%m-%d")).GetString(),
			//	todayDate.Format(_T("%Y-%m-%d")).GetString(),
			//	nPeriodicity);
			//OutputDebugString(debugMsg);

			// Apply different logic for Daily vs Intraday data
			if (nPeriodicity == 60)  // 1-minute data
			{
				const int MAX_BACKFILL_DAYS_1M = 30;  // Safety limit for 1-minute data

				// Check if data is too old → Force initial load
				if (gapDays > MAX_BACKFILL_DAYS_1M)
				{
					// Data too stale - do fresh 30-day load
					startTime = todayDate - CTimeSpan(MAX_BACKFILL_DAYS_1M, 0, 0, 0);
				}
				else
				{
					// Recent data - backfill from last bar's date
					startTime = lastBarDate;
				}
			}
			else  // Daily data (nPeriodicity == 86400)
			{
				const int MAX_BACKFILL_DAYS_DAILY = 730;     // Safety limit: 2 years
				const int MIN_DAILY_BARS = 250;               // ~1 year of trading days
				const int STALENESS_THRESHOLD_DAYS = 365;    // 1 year staleness check

				// CHECK 1: Do we have enough bars for proper analysis?
				if (lastMatchingBarIndex < MIN_DAILY_BARS)
				{
					// Too few Daily bars - do initial 10-year load
					startTime = todayDate - CTimeSpan(3650, 0, 0, 0);
				}
				// CHECK 2: Is data too stale?
				else if (gapDays > STALENESS_THRESHOLD_DAYS)
				{
					// Gap > 1 year - do initial 10-year load
					startTime = todayDate - CTimeSpan(3650, 0, 0, 0);
				}
				// CHECK 3: Gap within safety limit?
				else if (gapDays > MAX_BACKFILL_DAYS_DAILY)
				{
					// Gap exceeds 2-year safety limit - cap at maximum
					startTime = todayDate - CTimeSpan(MAX_BACKFILL_DAYS_DAILY, 0, 0, 0);
				}
				else
				{
					// Good recent data - backfill from last bar's date
					startTime = lastBarDate;
				}
			}
		}
		else
		{
			// ============================================================
			// NO EXISTING DATA - Initial Backfill
			// ============================================================

			// Check if manual backfill was requested
			if (g_bBackfillRequested && g_nBackfillPeriodicity == nPeriodicity && g_nBackfillDays > 0)
			{
				// Use requested backfill period
				startTime = todayDate - CTimeSpan((LONG)g_nBackfillDays, 0, 0, 0);
				g_bBackfillRequested = FALSE;  // Clear the request
			}
			else if (nPeriodicity == 60)  // 1-minute data
			{
				// Initial load: 30 days of 1-minute data
				startTime = todayDate - CTimeSpan(30, 0, 0, 0);
			}
			else  // Daily data
			{
				// Initial load: 10 years of daily data (increased from 1 year)
				// This provides sufficient historical context for technical analysis
				startTime = todayDate - CTimeSpan(3650, 0, 0, 0);
			}
		}

skip_gap_detection:

		CString startDate = startTime.Format(_T("%Y-%m-%d"));
		CString endDate = endTime.Format(_T("%Y-%m-%d"));

		// DEBUG: Log API request details
		//CString apiDebugMsg;
		//apiDebugMsg.Format(_T("API REQUEST - Symbol: %s, Exchange: %s, Interval: %s, Start: %s, End: %s"),
		//	(LPCTSTR)symbol, (LPCTSTR)exchange, (LPCTSTR)interval, (LPCTSTR)startDate, (LPCTSTR)endDate);
		//OutputDebugString(apiDebugMsg);

		CString oPostData;
		oPostData.Format(_T("{\"apikey\":\"%s\",\"symbol\":\"%s\",\"exchange\":\"%s\",\"interval\":\"%s\",\"start_date\":\"%s\",\"end_date\":\"%s\"}"),
			(LPCTSTR)g_oApiKey, (LPCTSTR)symbol, (LPCTSTR)exchange, (LPCTSTR)interval, (LPCTSTR)startDate, (LPCTSTR)endDate);

		CInternetSession oSession(AGENT_NAME, 1, INTERNET_OPEN_TYPE_DIRECT, NULL, NULL,
			INTERNET_FLAG_DONT_CACHE);
		oSession.SetOption(INTERNET_OPTION_CONNECT_TIMEOUT, 10000);
		oSession.SetOption(INTERNET_OPTION_RECEIVE_TIMEOUT, 10000);

		CHttpConnection* pConnection = NULL;
		CHttpFile* pFile = NULL;

		// Parse server
		INTERNET_PORT nPort = (INTERNET_PORT)g_nPortNumber;
		CString oServer = g_oServer;
		oServer.Replace(_T("http://"), _T(""));
		oServer.Replace(_T("https://"), _T(""));

		pConnection = oSession.GetHttpConnection(oServer, nPort);

		if (pConnection)
		{
			pFile = pConnection->OpenRequest(
				CHttpConnection::HTTP_VERB_POST,
				_T("/api/v1/history"),
				NULL, 1, NULL, NULL,
				INTERNET_FLAG_RELOAD | INTERNET_FLAG_DONT_CACHE);

			if (pFile)
			{
				CString oHeaders = _T("Content-Type: application/json\r\n");
				CStringA oPostDataA(oPostData);

				if (pFile->SendRequest(oHeaders, (LPVOID)(LPCSTR)oPostDataA, oPostDataA.GetLength()))
				{
					DWORD dwStatusCode = 0;
					pFile->QueryInfoStatusCode(dwStatusCode);

					if (dwStatusCode == 200)
					{
						CString oResponse;
						CString oLine;
						while (pFile->ReadString(oLine))
						{
							oResponse += oLine;
						}

						// Parse JSON response
						if (oResponse.Find(_T("\"status\":\"success\"")) >= 0)
						{
							// Find data array
							int dataStart = oResponse.Find(_T("\"data\":["));
							if (dataStart >= 0)
							{
								dataStart += 8;
								int dataEnd = oResponse.Find(_T("]"), dataStart);
								if (dataEnd < 0) dataEnd = oResponse.GetLength();
								CString dataArray = oResponse.Mid(dataStart, dataEnd - dataStart);

								// DEBUG: Log data received
								//CString dataDebugMsg;
								//dataDebugMsg.Format(_T("API RESPONSE - DataLength: %d bytes"), dataArray.GetLength());
								//OutputDebugString(dataDebugMsg);

								// Debug: Check if we have meaningful data
								if (dataArray.GetLength() < 10)
								{
									// Very little data, might be an issue
									//OutputDebugString(_T("WARNING: Very little data received from API"));
									return nLastValid + 1;
								}

								// Parse each candle and merge with existing data
								int quoteIndex = 0;
								int pos = 0;
								int originalLastValid = nLastValid;  // Save for accurate counting

								// If we have existing data, we'll need to merge properly
								BOOL bHasExistingData = (nLastValid >= 0);
								if (bHasExistingData)
								{
									// CRITICAL: Check if array is near full before adding new data
									// If array is 95% full, remove oldest 10% to make room for new bars
									const int ARRAY_THRESHOLD = (int)(nSize * 0.95);  // 95% full
									const int BARS_TO_REMOVE = (int)(nSize * 0.10);   // Remove 10%

									if (nLastValid >= ARRAY_THRESHOLD)
									{
										// Array is nearly full - shift data to remove oldest bars
										memmove(pQuotes, pQuotes + BARS_TO_REMOVE, (nLastValid - BARS_TO_REMOVE + 1) * sizeof(struct Quotation));
										nLastValid -= BARS_TO_REMOVE;

										// DEBUG: Log array cleanup
										//CString cleanupMsg;
										//cleanupMsg.Format(_T("ARRAY CLEANUP - Removed %d oldest bars, New nLastValid: %d"), BARS_TO_REMOVE, nLastValid);
										//OutputDebugString(cleanupMsg);
									}

									// Start appending after existing data
									quoteIndex = nLastValid + 1;
								}

								// Count duplicates for debugging
								int duplicateCount = 0;
								int uniqueCount = 0;

								while (pos < dataArray.GetLength() && quoteIndex < nSize)
								{
									int candleStart = dataArray.Find(_T("{"), pos);
									if (candleStart < 0) break;

									int candleEnd = dataArray.Find(_T("}"), candleStart);
									if (candleEnd < 0) break;

									// OPTIMIZED PARSING: Parse directly from dataArray without creating substrings
									
									// 1. Parse timestamp
									int tsPos = dataArray.Find(_T("\"timestamp\":"), candleStart);
									if (tsPos >= 0 && tsPos < candleEnd)
									{
										tsPos += 12; // Skip "timestamp":
										
										// Determine where the value ends
										int tsEnd = dataArray.Find(_T(","), tsPos);
										if (tsEnd < 0 || tsEnd > candleEnd) tsEnd = candleEnd; // Ends at closing brace or comma
										
										// Check if it's a string timestamp (starts with quote)
										if (dataArray[tsPos] == '\"')
										{
											// Handle string timestamp "2023-..." (Need to extract logic if complex, but assuming numeric for now or simplified)
											// For strictly numeric timestamps (Unix ms), this works. 
											// If API sends string, we might need a small buffer.
											// Given previous logic used _tstoi64, let's stick to simple extraction if safely possible.
											// But _tstoi64 stops at non-digit. Quote is non-digit.
											// So we need to advance if it's a quote.
											tsPos++; 
										}
										
										// Use pointer arithmetic for conversion - no allocation!
										time_t timestamp = (time_t)_tstoi64((LPCTSTR)dataArray + tsPos);

										// Convert to AmiBroker date
										if (nPeriodicity == 86400) // Daily data
										{
											// For daily data, set the DAILY_MASK and EOD markers
											ConvertUnixToPackedDate(timestamp, &pQuotes[quoteIndex].DateTime);
											pQuotes[quoteIndex].DateTime.Date |= DAILY_MASK;

											// Set EOD markers and normalize ALL time fields
											pQuotes[quoteIndex].DateTime.PackDate.Hour = 31;      // EOD marker
											pQuotes[quoteIndex].DateTime.PackDate.Minute = 63;    // EOD marker
											pQuotes[quoteIndex].DateTime.PackDate.Second = 0;     // Normalize
											pQuotes[quoteIndex].DateTime.PackDate.MilliSec = 0;   // Normalize
											pQuotes[quoteIndex].DateTime.PackDate.MicroSec = 0;   // Normalize
										}
										else
										{
											// For intraday data
											ConvertUnixToPackedDate(timestamp, &pQuotes[quoteIndex].DateTime);

											// CRITICAL FIX: Normalize sub-minute time fields for 1-minute bars
											if (nPeriodicity == 60) // 1-minute data
											{
												pQuotes[quoteIndex].DateTime.PackDate.Second = 0;     // Normalize
												pQuotes[quoteIndex].DateTime.PackDate.MilliSec = 0;   // Normalize
												pQuotes[quoteIndex].DateTime.PackDate.MicroSec = 0;   // Normalize
											}
										}

										// Parse OHLCV using direct pointer arithmetic
										// Helper macro or lambda would be cleaner but keeping it inline for C++ compatibility
										
										// Open
										int oPos = dataArray.Find(_T("\"open\":"), candleStart);
										if (oPos >= 0 && oPos < candleEnd)
											pQuotes[quoteIndex].Open = (float)_tcstod((LPCTSTR)dataArray + oPos + 7, NULL);

										// High
										int hPos = dataArray.Find(_T("\"high\":"), candleStart);
										if (hPos >= 0 && hPos < candleEnd)
											pQuotes[quoteIndex].High = (float)_tcstod((LPCTSTR)dataArray + hPos + 7, NULL);

										// Low
										int lPos = dataArray.Find(_T("\"low\":"), candleStart);
										if (lPos >= 0 && lPos < candleEnd)
											pQuotes[quoteIndex].Low = (float)_tcstod((LPCTSTR)dataArray + lPos + 6, NULL);

										// Close/Price
										int cPos = dataArray.Find(_T("\"close\":"), candleStart);
										if (cPos >= 0 && cPos < candleEnd)
											pQuotes[quoteIndex].Price = (float)_tcstod((LPCTSTR)dataArray + cPos + 8, NULL);

										// Volume
										int vPos = dataArray.Find(_T("\"volume\":"), candleStart);
										if (vPos >= 0 && vPos < candleEnd)
											pQuotes[quoteIndex].Volume = (float)_tcstod((LPCTSTR)dataArray + vPos + 9, NULL);

										// OI
										int oiPos = dataArray.Find(_T("\"oi\":"), candleStart);
										if (oiPos >= 0 && oiPos < candleEnd)
											pQuotes[quoteIndex].OpenInterest = (float)_tcstod((LPCTSTR)dataArray + oiPos + 5, NULL);

										// Set auxiliary data
										pQuotes[quoteIndex].AuxData1 = 0;
										pQuotes[quoteIndex].AuxData2 = 0;

										// Check for duplicate timestamps against existing data
										// (This logic remains logically same but optimized slightly)
										BOOL bIsDuplicate = FALSE;
										if (bHasExistingData)
										{
											// Optimize checking: Check against last added bar first (common case)
											if (quoteIndex > 0 && quoteIndex > (nLastValid + 1))
											{
												// We are appending new data to new data - check strictly strictly increasing
												// If needed, but usually API returns unique sorted data.
											}

											// Legacy check logic for merging with OLD data
											BOOL bNewBarIsEOD = (pQuotes[quoteIndex].DateTime.PackDate.Hour == DATE_EOD_HOURS &&
											                     pQuotes[quoteIndex].DateTime.PackDate.Minute == DATE_EOD_MINUTES);

											unsigned int newBarYear = pQuotes[quoteIndex].DateTime.PackDate.Year;
											unsigned int newBarMonth = pQuotes[quoteIndex].DateTime.PackDate.Month;
											unsigned int newBarDay = pQuotes[quoteIndex].DateTime.PackDate.Day;

											// Check duplicates in REVERSE from nLastValid
											// Performance critical loop
											for (int i = nLastValid; i >= 0; i--)
											{
												// Optimization: Stop if we go too far back in time
												if (pQuotes[i].DateTime.PackDate.Year < newBarYear) break;
												if (pQuotes[i].DateTime.PackDate.Year == newBarYear)
												{
													if (pQuotes[i].DateTime.PackDate.Month < newBarMonth) break;
													if (pQuotes[i].DateTime.PackDate.Month == newBarMonth && pQuotes[i].DateTime.PackDate.Day < newBarDay) break;
												}

												BOOL bExistingBarIsEOD = (pQuotes[i].DateTime.PackDate.Hour == DATE_EOD_HOURS &&
												                          pQuotes[i].DateTime.PackDate.Minute == DATE_EOD_MINUTES);

												if (bNewBarIsEOD != bExistingBarIsEOD) continue;

												// Check match
												BOOL bSameBar = FALSE;
												if (bNewBarIsEOD)
												{
													bSameBar = (pQuotes[quoteIndex].DateTime.PackDate.Day == pQuotes[i].DateTime.PackDate.Day &&
													            pQuotes[quoteIndex].DateTime.PackDate.Month == pQuotes[i].DateTime.PackDate.Month &&
													            pQuotes[quoteIndex].DateTime.PackDate.Year == pQuotes[i].DateTime.PackDate.Year);
												}
												else
												{
													bSameBar = (pQuotes[quoteIndex].DateTime.PackDate.Minute == pQuotes[i].DateTime.PackDate.Minute &&
													            pQuotes[quoteIndex].DateTime.PackDate.Hour == pQuotes[i].DateTime.PackDate.Hour &&
													            pQuotes[quoteIndex].DateTime.PackDate.Day == pQuotes[i].DateTime.PackDate.Day &&
													            pQuotes[quoteIndex].DateTime.PackDate.Month == pQuotes[i].DateTime.PackDate.Month &&
													            pQuotes[quoteIndex].DateTime.PackDate.Year == pQuotes[i].DateTime.PackDate.Year);
												}

												if (bSameBar)
												{
													bIsDuplicate = TRUE;
													// Update existing
													pQuotes[i].Price = pQuotes[quoteIndex].Price;
													pQuotes[i].High = max(pQuotes[i].High, pQuotes[quoteIndex].High);
													pQuotes[i].Low = (pQuotes[i].Low == 0) ? pQuotes[quoteIndex].Low : min(pQuotes[i].Low, pQuotes[quoteIndex].Low);
													pQuotes[i].Volume = pQuotes[quoteIndex].Volume;
													pQuotes[i].OpenInterest = pQuotes[quoteIndex].OpenInterest;
													break;
												}
											}
										}

										// Only add new bar if it's not a duplicate
										if (!bIsDuplicate)
										{
											quoteIndex++;
											uniqueCount++;
										}
										else
										{
											duplicateCount++;
										}
									}

									pos = candleEnd + 1;
								}

								// DO NOT mix quote data with historical interval data
								// Quote data is for real-time window only, not for OHLC bars
								// Historical data from OpenAlgo is already complete and accurate

								pFile->Close();
								delete pFile;
								pConnection->Close();
								delete pConnection;
								oSession.Close();

								// CRITICAL: Sort quotes by timestamp (oldest to newest)
								// This ensures proper chronological order after merging new data with existing data
								// Without sorting, timestamps can be mixed up when filling gaps or adding historical data
								if (quoteIndex > 0)
								{
									qsort(pQuotes, quoteIndex, sizeof(struct Quotation), CompareQuotations);
								}

								// If we have more data than the array can hold, keep the most recent data
								if (quoteIndex > nSize)
								{
									int excessBars = quoteIndex - nSize;
									memmove(pQuotes, pQuotes + excessBars, nSize * sizeof(struct Quotation));
									quoteIndex = nSize;
								}

								// DEBUG: Log final result
								//CString resultDebugMsg;
								//resultDebugMsg.Format(_T("BACKFILL COMPLETE - Total: %d, Original: %d, Unique: %d, Duplicates: %d"),
								//	quoteIndex, originalLastValid + 1, uniqueCount, duplicateCount);
								//OutputDebugString(resultDebugMsg);

								return quoteIndex;
							}
						}
					}
				}

				if (pFile)
				{
					pFile->Close();
					delete pFile;
				}
			}

			if (pConnection)
			{
				pConnection->Close();
				delete pConnection;
			}
		}

		oSession.Close();
	}
	catch (CInternetException* e)
	{
		e->Delete();
	}

	return nLastValid + 1;
}

BOOL AddToOpenAlgoPortfolio(LPCTSTR pszTicker)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	BOOL bOK = FALSE;
	try
	{
		CString endpoint;
		endpoint.Format(_T("/api/v1/watchlist/add?symbol=%s"), pszTicker);
		CString oURL = BuildOpenAlgoURL(g_oServer, g_nPortNumber, endpoint);

		CInternetSession oSession(AGENT_NAME, 1, INTERNET_OPEN_TYPE_DIRECT, NULL, NULL, INTERNET_FLAG_DONT_CACHE);

		CStdioFile* poFile = oSession.OpenURL(oURL, 1, INTERNET_FLAG_TRANSFER_ASCII | INTERNET_FLAG_RELOAD | INTERNET_FLAG_DONT_CACHE);

		CString oLine;
		if (poFile && poFile->ReadString(oLine))
		{
			if (oLine.Find(_T("OK")) >= 0 || oLine.Find(_T("success")) >= 0)
			{
				bOK = TRUE;
			}
		}

		if (poFile)
		{
			poFile->Close();
			delete poFile;
		}
		oSession.Close();
	}
	catch (CInternetException* e)
	{
		e->Delete();
		g_nStatus = STATUS_DISCONNECTED;
	}
	return bOK;
}

///////////////////////////////////////////////////////////
// Exported Functions
///////////////////////////////////////////////////////////
PLUGINAPI int GetPluginInfo(struct PluginInfo* pInfo)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	if (pInfo == NULL) return FALSE;

	*pInfo = oPluginInfo;
	return TRUE;
}

PLUGINAPI int Init(void)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	// ALWAYS log initialization
	OutputDebugString(_T("OpenAlgo: Init() called"));

	if (!g_bPluginInitialized)
	{
		// Initialize on first call
		g_oServer = AfxGetApp()->GetProfileString(_T("OpenAlgo"), _T("Server"), _T("127.0.0.1"));
		g_oApiKey = AfxGetApp()->GetProfileString(_T("OpenAlgo"), _T("ApiKey"), _T(""));  // Load API Key
		g_oWebSocketUrl = AfxGetApp()->GetProfileString(_T("OpenAlgo"), _T("WebSocketUrl"), _T("ws://127.0.0.1:8765"));  // Load WebSocket URL
		g_nPortNumber = AfxGetApp()->GetProfileInt(_T("OpenAlgo"), _T("Port"), 5000);
		g_nRefreshInterval = AfxGetApp()->GetProfileInt(_T("OpenAlgo"), _T("RefreshInterval"), 5);
		g_nTimeShift = AfxGetApp()->GetProfileInt(_T("OpenAlgo"), _T("TimeShift"), 0);

		// Real-time candle building settings
		g_bRealTimeCandlesEnabled = AfxGetApp()->GetProfileInt(_T("OpenAlgo"), _T("EnableRealTimeCandles"), 1);  // Default: enabled
		g_nBackfillIntervalMs = AfxGetApp()->GetProfileInt(_T("OpenAlgo"), _T("BackfillIntervalMs"), 5000);  // Default: 5 seconds

		g_nStatus = STATUS_WAIT;
		g_bPluginInitialized = TRUE;

		// Initialize quote cache
		g_QuoteCache.InitHashTable(997); // Prime number for better hash distribution

		// Initialize critical section for WebSocket operations
		InitializeCriticalSection(&g_WebSocketCriticalSection);
		g_bCriticalSectionInitialized = TRUE;

		// Initialize critical section for BarBuilder operations
		InitializeCriticalSection(&g_BarBuilderCriticalSection);
		g_bBarBuilderCriticalSectionInitialized = TRUE;

		// Initialize BarBuilders hash table
		g_BarBuilders.InitHashTable(503);  // Prime number for better distribution

		// Initialize critical section for HTTP cache operations
		InitializeCriticalSection(&g_HttpCacheCriticalSection);
		g_bHttpCacheCriticalSectionInitialized = TRUE;

		// Initialize HTTP response cache hash table
		g_HttpResponseCache.InitHashTable(127);  // Prime number for better distribution

		// Log real-time settings
		CString rtMsg;
		rtMsg.Format(_T("OpenAlgo: Real-Time Candles Enabled = %d, Backfill Interval = %d ms"),
			g_bRealTimeCandlesEnabled, g_nBackfillIntervalMs);
		OutputDebugString(rtMsg);

		// Initialize WebSocket connection early (don't wait for GetRecentInfo)
		OutputDebugString(_T("OpenAlgo: Init() - Initializing WebSocket connection..."));
		if (InitializeWebSocket())
		{
			OutputDebugString(_T("OpenAlgo: Init() - WebSocket initialized successfully"));
		}
		else
		{
			OutputDebugString(_T("OpenAlgo: Init() - WebSocket initialization failed (will retry later)"));
		}
	}

	OutputDebugString(_T("OpenAlgo: Init() completed successfully"));
	return 1;
}

PLUGINAPI int Release(void)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	// Clean up WebSocket connections
	CleanupWebSocket();

	// Clear cache
	g_QuoteCache.RemoveAll();

	// Clean up BarBuilders
	CleanupBarBuilders();

	// Clean up critical sections
	if (g_bCriticalSectionInitialized)
	{
		DeleteCriticalSection(&g_WebSocketCriticalSection);
		g_bCriticalSectionInitialized = FALSE;
	}

	if (g_bBarBuilderCriticalSectionInitialized)
	{
		DeleteCriticalSection(&g_BarBuilderCriticalSection);
		g_bBarBuilderCriticalSectionInitialized = FALSE;
	}

	if (g_bHttpCacheCriticalSectionInitialized)
	{
		// Clean up HTTP cache - free allocated memory
		POSITION pos = g_HttpResponseCache.GetStartPosition();
		while (pos != NULL)
		{
			CString key;
			void* pValue;
			g_HttpResponseCache.GetNextAssoc(pos, key, pValue);
			if (pValue != NULL)
			{
				delete (DWORD*)pValue;
			}
		}
		g_HttpResponseCache.RemoveAll();

		DeleteCriticalSection(&g_HttpCacheCriticalSection);
		g_bHttpCacheCriticalSectionInitialized = FALSE;
	}

	return 1;
}

PLUGINAPI int Configure(LPCTSTR pszPath, struct InfoSite* pSite)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	COpenAlgoConfigDlg oDlg;
	oDlg.m_pSite = pSite;

	if (oDlg.DoModal() == IDOK)
	{
		// Force status update after config change
		if (g_hAmiBrokerWnd != NULL)
		{
			::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
		}
	}

	return 1;
}

PLUGINAPI AmiVar GetExtraData(LPCTSTR pszTicker, LPCTSTR pszName, int nArraySize, int nPeriodicity, void* (*pfAlloc)(unsigned int nSize))
{
	AmiVar var;
	var.type = VAR_NONE;
	var.val = 0;
	return var;
}

PLUGINAPI int SetTimeBase(int nTimeBase)
{
	return 1;
}

PLUGINAPI int GetSymbolLimit(void)
{
	return 1000; // Default symbol limit since we removed the configurable option
}

// CRITICAL FUNCTION - This makes the status LED work!
PLUGINAPI int GetStatus(struct PluginStatus* status)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	if (status == NULL) return 0;

	// MUST set structure size
	status->nStructSize = sizeof(struct PluginStatus);

	// Ensure we have valid status
	if (g_nStatus < STATUS_WAIT || g_nStatus > STATUS_SHUTDOWN)
	{
		g_nStatus = STATUS_WAIT;
	}

	switch (g_nStatus)
	{
	case STATUS_WAIT:
		status->nStatusCode = 0x10000000; // WARNING
		strcpy_s(status->szShortMessage, 32, "WAIT");
		strcpy_s(status->szLongMessage, 256, "OpenAlgo: Waiting to connect");
		status->clrStatusColor = RGB(255, 255, 0); // Yellow
		break;

	case STATUS_CONNECTED:
		status->nStatusCode = 0x00000000; // OK
		strcpy_s(status->szShortMessage, 32, "OK");
		strcpy_s(status->szLongMessage, 256, "OpenAlgo: Connected");
		status->clrStatusColor = RGB(0, 255, 0); // Green
		break;

	case STATUS_DISCONNECTED:
		status->nStatusCode = 0x20000000; // MINOR ERROR
		strcpy_s(status->szShortMessage, 32, "ERR");
		strcpy_s(status->szLongMessage, 256, "OpenAlgo: Connection failed. Will retry in 15 seconds.");
		status->clrStatusColor = RGB(255, 0, 0); // Red
		break;

	case STATUS_SHUTDOWN:
		status->nStatusCode = 0x30000000; // SEVERE ERROR
		strcpy_s(status->szShortMessage, 32, "OFF");
		strcpy_s(status->szLongMessage, 256, "OpenAlgo: Offline. Right-click to reconnect.");
		status->clrStatusColor = RGB(192, 0, 192); // Purple
		break;

	default:
		status->nStatusCode = 0x30000000; // SEVERE ERROR
		strcpy_s(status->szShortMessage, 32, "???");
		strcpy_s(status->szLongMessage, 256, "OpenAlgo: Unknown status");
		status->clrStatusColor = RGB(128, 128, 128); // Gray
		break;
	}

	return 1; // MUST return 1!
}

CString GetAvailableSymbols(void)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	CString oResult;
	try
	{
		CString oURL = BuildOpenAlgoURL(g_oServer, g_nPortNumber, _T("/api/v1/symbols"));

		CInternetSession oSession(AGENT_NAME, 1, INTERNET_OPEN_TYPE_DIRECT, NULL, NULL, INTERNET_FLAG_DONT_CACHE);
		oSession.SetOption(INTERNET_OPTION_CONNECT_TIMEOUT, 5000);
		oSession.SetOption(INTERNET_OPTION_RECEIVE_TIMEOUT, 5000);

		CStdioFile* poFile = oSession.OpenURL(oURL, 1,
			INTERNET_FLAG_TRANSFER_ASCII | INTERNET_FLAG_RELOAD | INTERNET_FLAG_DONT_CACHE);

		if (poFile)
		{
			CString oLine;
			if (poFile->ReadString(oLine))
			{
				if (oLine.Left(2) == _T("OK"))
				{
					poFile->ReadString(oResult);
				}
				else
				{
					oResult = oLine;
				}
			}
			poFile->Close();
			delete poFile;
		}
		oSession.Close();
	}
	catch (CInternetException* e)
	{
		e->Delete();
		g_nStatus = STATUS_DISCONNECTED;
	}
	return oResult;
}

BOOL TestOpenAlgoConnection(void)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	// Check if API key is configured
	if (g_oApiKey.IsEmpty())
	{
		return FALSE;
	}

	BOOL bConnected = FALSE;

	try
	{
		CString oURL = BuildOpenAlgoURL(g_oServer, g_nPortNumber, _T("/api/v1/ping"));

		CInternetSession oSession(AGENT_NAME, 1, INTERNET_OPEN_TYPE_DIRECT, NULL, NULL,
			INTERNET_FLAG_DONT_CACHE);
		oSession.SetOption(INTERNET_OPTION_CONNECT_TIMEOUT, 2000);
		oSession.SetOption(INTERNET_OPTION_RECEIVE_TIMEOUT, 2000);

		// Prepare POST data with API key
		CString oPostData;
		oPostData.Format(_T("{\"apikey\":\"%s\"}"), (LPCTSTR)g_oApiKey);

		CHttpConnection* pConnection = NULL;
		CHttpFile* pFile = NULL;

		// Parse server and port
		INTERNET_PORT nPort = (INTERNET_PORT)g_nPortNumber;
		CString oServer = g_oServer;

		// Remove http:// or https:// if present
		oServer.Replace(_T("http://"), _T(""));
		oServer.Replace(_T("https://"), _T(""));

		pConnection = oSession.GetHttpConnection(oServer, nPort);

		if (pConnection)
		{
			// Create POST request
			pFile = pConnection->OpenRequest(
				CHttpConnection::HTTP_VERB_POST,
				_T("/api/v1/ping"),
				NULL,
				1,
				NULL,
				NULL,
				INTERNET_FLAG_RELOAD | INTERNET_FLAG_DONT_CACHE);

			if (pFile)
			{
				// Set headers
				CString oHeaders = _T("Content-Type: application/json\r\n");

				// Convert string to UTF-8 for sending
				CStringA oPostDataA(oPostData);

				// Send the request
				BOOL bResult = pFile->SendRequest(oHeaders, (LPVOID)(LPCSTR)oPostDataA, oPostDataA.GetLength());

				if (bResult)
				{
					DWORD dwStatusCode = 0;
					pFile->QueryInfoStatusCode(dwStatusCode);

					if (dwStatusCode == 200)
					{
						// Read response to verify it's valid
						CString oResponse;
						CString oLine;
						while (pFile->ReadString(oLine))
						{
							oResponse += oLine;
							if (oResponse.GetLength() > 500) break; // Limit response size
						}

						// Check if response contains "success" and "pong"
						if ((oResponse.Find(_T("\"status\":\"success\"")) >= 0 ||
							 oResponse.Find(_T("\"status\": \"success\"")) >= 0) &&
							(oResponse.Find(_T("\"message\":\"pong\"")) >= 0 ||
							 oResponse.Find(_T("\"message\": \"pong\"")) >= 0))
						{
							bConnected = TRUE;
						}
					}
				}

				pFile->Close();
				delete pFile;
			}

			pConnection->Close();
			delete pConnection;
		}

		oSession.Close();
	}
	catch (CInternetException* e)
	{
		e->Delete();
		bConnected = FALSE;
	}

	return bConnected;
}

void SetupRetry(void)
{
	if (--g_nRetryCount > 0)
	{
		if (g_hAmiBrokerWnd != NULL)
		{
			SetTimer(g_hAmiBrokerWnd, TIMER_INIT, 15000, (TIMERPROC)OnTimerProc);
		}
		g_nStatus = STATUS_DISCONNECTED;
	}
	else
	{
		g_nStatus = STATUS_SHUTDOWN;
	}

	if (g_hAmiBrokerWnd != NULL)
	{
		::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
	}
}

VOID CALLBACK OnTimerProc(HWND hwnd, UINT uMsg, UINT_PTR idEvent, DWORD dwTime)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	// High-frequency WebSocket data processing timer (every 100ms)
	if (idEvent == TIMER_WEBSOCKET)
	{
		// ALWAYS call ProcessWebSocketData() if real-time candles are enabled
		// ProcessWebSocketData() has its own auto-reconnect logic when disconnected
		// DO NOT check g_bWebSocketConnected here, or auto-reconnect will never run!
		if (g_bRealTimeCandlesEnabled)
		{
			ProcessWebSocketData();
		}
		return;
	}

	if (idEvent == TIMER_INIT || idEvent == TIMER_REFRESH)
	{
		if (!TestOpenAlgoConnection())
		{
			if (g_hAmiBrokerWnd != NULL)
			{
				KillTimer(g_hAmiBrokerWnd, idEvent);
			}
			SetupRetry();
			return;
		}

		g_nStatus = STATUS_CONNECTED;
		g_nRetryCount = RETRY_COUNT;

		if (g_hAmiBrokerWnd != NULL)
		{
			::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);

			if (idEvent == TIMER_INIT)
			{
				KillTimer(g_hAmiBrokerWnd, TIMER_INIT);
				SetTimer(g_hAmiBrokerWnd, TIMER_REFRESH, g_nRefreshInterval * 1000, (TIMERPROC)OnTimerProc);
			}
		}
	}
}

PLUGINAPI int Notify(struct PluginNotification* pn)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	if (pn == NULL) return 0;

	// Database loaded - start connection
	if ((pn->nReason & REASON_DATABASE_LOADED))
	{
		g_hAmiBrokerWnd = pn->hMainWnd;

		// Reload settings
		g_oServer = AfxGetApp()->GetProfileString(_T("OpenAlgo"), _T("Server"), _T("127.0.0.1"));
		g_oApiKey = AfxGetApp()->GetProfileString(_T("OpenAlgo"), _T("ApiKey"), _T(""));  // Load API Key
		g_oWebSocketUrl = AfxGetApp()->GetProfileString(_T("OpenAlgo"), _T("WebSocketUrl"), _T("ws://127.0.0.1:8765"));  // Load WebSocket URL
		g_nPortNumber = AfxGetApp()->GetProfileInt(_T("OpenAlgo"), _T("Port"), 5000);
		g_nRefreshInterval = AfxGetApp()->GetProfileInt(_T("OpenAlgo"), _T("RefreshInterval"), 5);

		g_nStatus = STATUS_WAIT;
		g_nRetryCount = RETRY_COUNT;

		// Start connection timer
		if (g_hAmiBrokerWnd != NULL)
		{
			SetTimer(g_hAmiBrokerWnd, TIMER_INIT, 1000, (TIMERPROC)OnTimerProc);

			// Start high-frequency WebSocket timer (100ms) for continuous tick processing
			// This ensures we read WebSocket data continuously, not just when GetQuotesEx() is called
			if (g_bRealTimeCandlesEnabled)
			{
				SetTimer(g_hAmiBrokerWnd, TIMER_WEBSOCKET, 100, (TIMERPROC)OnTimerProc);
				OutputDebugString(_T("OpenAlgo: Started TIMER_WEBSOCKET (100ms) for continuous tick processing"));
			}

			// Force immediate status update
			::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
		}
	}

	// Database unloaded - cleanup
	if (pn->nReason & REASON_DATABASE_UNLOADED)
	{
		if (g_hAmiBrokerWnd != NULL)
		{
			KillTimer(g_hAmiBrokerWnd, TIMER_INIT);
			KillTimer(g_hAmiBrokerWnd, TIMER_REFRESH);
			KillTimer(g_hAmiBrokerWnd, TIMER_WEBSOCKET);
		}
		g_hAmiBrokerWnd = NULL;
		g_nStatus = STATUS_SHUTDOWN;

		free(g_aInfos);
		g_aInfos = NULL;
		RecentInfoSize = 0;

		// Clear cache
		g_QuoteCache.RemoveAll();
	}

	// Right-click on status area - show menu
	if (pn->nReason & REASON_STATUS_RMBCLICK)
	{
		HWND hWnd = pn->hMainWnd ? pn->hMainWnd : g_hAmiBrokerWnd;
		if (hWnd != NULL)
		{
			HMENU hMenu = CreatePopupMenu();

			// Connection control
			if (g_nStatus == STATUS_SHUTDOWN || g_nStatus == STATUS_DISCONNECTED)
			{
				AppendMenu(hMenu, MF_STRING | MF_ENABLED, 1, _T("Connect"));
			}
			else
			{
				AppendMenu(hMenu, MF_STRING | MF_ENABLED, 2, _T("Disconnect"));
			}

			AppendMenu(hMenu, MF_SEPARATOR, 0, NULL);

			// Backfill submenu for Intraday (1-minute)
			HMENU hBackfill1M = CreatePopupMenu();
			AppendMenu(hBackfill1M, MF_STRING | MF_ENABLED, 105, _T("3 Months (Current Symbol)"));
			AppendMenu(hBackfill1M, MF_STRING | MF_ENABLED, 106, _T("3 Months (All Symbols)"));
			AppendMenu(hBackfill1M, MF_STRING | MF_ENABLED, 107, _T("6 Months (Current Symbol)"));
			AppendMenu(hBackfill1M, MF_STRING | MF_ENABLED, 108, _T("6 Months (All Symbols)"));
			AppendMenu(hBackfill1M, MF_STRING | MF_ENABLED, 109, _T("1 Year (Current Symbol)"));
			AppendMenu(hBackfill1M, MF_STRING | MF_ENABLED, 110, _T("1 Year (All Symbols)"));

			// Backfill submenu for Daily (EOD)
			HMENU hBackfillDaily = CreatePopupMenu();
			AppendMenu(hBackfillDaily, MF_STRING | MF_ENABLED, 201, _T("5 Years (Current Symbol)"));
			AppendMenu(hBackfillDaily, MF_STRING | MF_ENABLED, 202, _T("5 Years (All Symbols)"));
			AppendMenu(hBackfillDaily, MF_STRING | MF_ENABLED, 203, _T("10 Years (Current Symbol)"));
			AppendMenu(hBackfillDaily, MF_STRING | MF_ENABLED, 204, _T("10 Years (All Symbols)"));
			AppendMenu(hBackfillDaily, MF_STRING | MF_ENABLED, 205, _T("25 Years (Current Symbol)"));
			AppendMenu(hBackfillDaily, MF_STRING | MF_ENABLED, 206, _T("25 Years (All Symbols)"));

			// Add submenus to main menu
			AppendMenu(hMenu, MF_POPUP | MF_ENABLED, (UINT_PTR)hBackfill1M, _T("Backfill 1-Minute Data"));
			AppendMenu(hMenu, MF_POPUP | MF_ENABLED, (UINT_PTR)hBackfillDaily, _T("Backfill Daily Data"));

			AppendMenu(hMenu, MF_SEPARATOR, 0, NULL);
			AppendMenu(hMenu, MF_STRING | MF_ENABLED, 3, _T("Configure..."));

			POINT pt;
			GetCursorPos(&pt);

			int nCmd = TrackPopupMenu(hMenu, TPM_RETURNCMD | TPM_LEFTALIGN | TPM_TOPALIGN | TPM_LEFTBUTTON | TPM_NONOTIFY,
				pt.x, pt.y, 0, hWnd, NULL);

			// Destroy menus after use (submenus are destroyed automatically with parent)
			DestroyMenu(hMenu);

			switch (nCmd)
			{
			case 1: // Connect
				g_nStatus = STATUS_WAIT;
				g_nRetryCount = RETRY_COUNT;
				SetTimer(g_hAmiBrokerWnd, TIMER_INIT, 1000, (TIMERPROC)OnTimerProc);
				break;

			case 2: // Disconnect
				KillTimer(g_hAmiBrokerWnd, TIMER_INIT);
				KillTimer(g_hAmiBrokerWnd, TIMER_REFRESH);
				g_nStatus = STATUS_SHUTDOWN;
				break;

			case 3: // Configure
				Configure(pn->pszDatabasePath, NULL);
				break;

			// 1-Minute backfill options
			case 105: // 3 Months - Current Symbol
				g_nBackfillDays = 90;
				g_nBackfillPeriodicity = 60;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 106: // 3 Months - All Symbols
				g_nBackfillDays = 90;
				g_nBackfillPeriodicity = 60;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 107: // 6 Months - Current Symbol
				g_nBackfillDays = 180;
				g_nBackfillPeriodicity = 60;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 108: // 6 Months - All Symbols
				g_nBackfillDays = 180;
				g_nBackfillPeriodicity = 60;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 109: // 1 Year - Current Symbol
				g_nBackfillDays = 365;
				g_nBackfillPeriodicity = 60;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 110: // 1 Year - All Symbols
				g_nBackfillDays = 365;
				g_nBackfillPeriodicity = 60;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;

			// Daily backfill options
			case 201: // 5 Years - Current Symbol
				g_nBackfillDays = 1825;  // 5 * 365
				g_nBackfillPeriodicity = 86400;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 202: // 5 Years - All Symbols
				g_nBackfillDays = 1825;
				g_nBackfillPeriodicity = 86400;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 203: // 10 Years - Current Symbol
				g_nBackfillDays = 3650;  // 10 * 365
				g_nBackfillPeriodicity = 86400;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 204: // 10 Years - All Symbols
				g_nBackfillDays = 3650;
				g_nBackfillPeriodicity = 86400;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 205: // 25 Years - Current Symbol
				g_nBackfillDays = 9125;  // 25 * 365
				g_nBackfillPeriodicity = 86400;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			case 206: // 25 Years - All Symbols
				g_nBackfillDays = 9125;
				g_nBackfillPeriodicity = 86400;
				g_bBackfillRequested = TRUE;
				::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
				break;
			}

			// Update status display
			::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
		}
	}

	return 1;
}

PLUGINAPI int GetQuotes(LPCTSTR pszTicker, int nPeriodicity, int nLastValid, int nSize, struct QuotationFormat4* pQuotes)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	Quotation* pQuote5 = (struct Quotation*)malloc(nSize * sizeof(Quotation));
	QuotationFormat4* src = pQuotes;
	Quotation* dst = pQuote5;

	int i;
	for (i = 0; i <= nLastValid; i++, src++, dst++)
	{
		ConvertFormat4Quote(src, dst);
	}

	int nQty = GetQuotesEx(pszTicker, nPeriodicity, nLastValid, nSize, pQuote5, NULL);

	dst = pQuote5;
	src = pQuotes;

	for (i = 0; i < nQty; i++, dst++, src++)
	{
		ConvertFormat5Quote(dst, src);
	}

	free(pQuote5);
	return nQty;
}

// Main quote retrieval function - ZERO exchange restrictions
// Handles ALL trading scenarios dynamically through OpenAlgo server
PLUGINAPI int GetQuotesEx(LPCTSTR pszTicker, int nPeriodicity, int nLastValid, int nSize, struct Quotation* pQuotes, GQEContext* pContext)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	if (g_nStatus == STATUS_DISCONNECTED || g_nStatus == STATUS_SHUTDOWN)
	{
		return nLastValid + 1;
	}

	// Handle Daily (EOD) data separately
	if (nPeriodicity == 86400) // Daily (24 * 60 * 60 seconds)
	{
		// For daily data, only use historical data from OpenAlgo
		// Do NOT mix with quote data as it's for real-time window only
		int nQty = GetOpenAlgoHistory(pszTicker, nPeriodicity, nLastValid, nSize, pQuotes);
		return nQty;
	}
	// Handle intraday data (1-minute only for now)
	else if (nPeriodicity == 60)
	{
		// MIXED EOD/INTRADAY SUPPORT:
		// When base interval is 1-minute, AmiBroker only calls GetQuotesEx(ticker, 60).
		// It compresses 1m data to create 5m, 15m, Daily charts automatically.
		// But in Mixed EOD mode (AllowMixedEODIntra = TRUE), we need BOTH:
		//   - Daily EOD data (10 years) - OLDEST, stored first
		//   - Intraday data (1m, 30 days) - NEWEST, stored after Daily
		//
		// CRITICAL: Fetch in chronological order!
		// AmiBroker requires array sorted oldest→newest:
		//   [0...2473]    : Daily bars (2015-2025)
		//   [2474...10033]: 1-minute bars (last 30 days)

		int nQty = nLastValid + 1;

		// Step 1: Check if Daily EOD data exists
		// Use FindLastBarOfMatchingType to scan for bars with Hour=31, Minute=63
		int lastDailyBarIndex = FindLastBarOfMatchingType(86400, nLastValid, pQuotes);

		if (lastDailyBarIndex < 0)
		{
			// No Daily data found - Fetch Daily data FIRST (chronologically oldest)
			// This puts 10 years of Daily bars at the beginning of the array
			nQty = GetOpenAlgoHistory(pszTicker, 86400, nLastValid, nSize, pQuotes);
		}
		else if (lastDailyBarIndex < 250)
		{
			// Found some Daily data but < 250 bars (~1 year)
			// Insufficient for technical analysis - fetch full 10 years FIRST
			nQty = GetOpenAlgoHistory(pszTicker, 86400, nLastValid, nSize, pQuotes);
		}
		// else: Daily data exists and is sufficient (>= 250 bars)

		// Step 2: Fetch 1-minute intraday data (chronologically newest)
		// This appends after Daily data, maintaining chronological order
		//
		// REAL-TIME ENHANCEMENT: If real-time candles enabled, merge with tick bars
		if (g_bRealTimeCandlesEnabled)
		{
			static int s_gqeCallCount = 0;
			s_gqeCallCount++;

			CString gqeLog;
			gqeLog.Format(_T("OpenAlgo: GetQuotesEx() #%d called for %s (periodicity=%d)"),
				s_gqeCallCount, pszTicker, nPeriodicity);
			OutputDebugString(gqeLog);

			// NOTE: WebSocket data is now processed by TIMER_WEBSOCKET (every 100ms)
			// No need to call ProcessWebSocketData() here - it runs continuously in background
			// This ensures ticks are processed immediately when they arrive, not just when GetQuotesEx() is called

			CString ticker(pszTicker);
			BarBuilder* pBuilder = NULL;

			// CRITICAL: Subscribe to symbol if not already subscribed
			// This ensures chart-only symbols (without quote window) also get ticks
			BOOL bSubscribed = FALSE;
			EnterCriticalSection(&g_WebSocketCriticalSection);
			if (!g_SubscribedSymbols.Lookup(ticker, bSubscribed))
			{
				OutputDebugString(_T("OpenAlgo: GetQuotesEx - Symbol NOT subscribed, subscribing now..."));
				if (g_bWebSocketConnected && SubscribeToSymbol(pszTicker))
				{
					g_SubscribedSymbols.SetAt(ticker, TRUE);
					OutputDebugString(_T("OpenAlgo: GetQuotesEx - Successfully subscribed to symbol"));
				}
				else
				{
					OutputDebugString(_T("OpenAlgo: GetQuotesEx - WARNING: Failed to subscribe to symbol"));
				}
			}
			LeaveCriticalSection(&g_WebSocketCriticalSection);

			// Check if we have a BarBuilder for this symbol
			if (g_BarBuilders.Lookup(ticker, pBuilder) && pBuilder != NULL)
			{
				OutputDebugString(_T("OpenAlgo: GetQuotesEx - BarBuilder found, entering critical section"));
				EnterCriticalSection(&g_BarBuilderCriticalSection);

				// PERFORMANCE FIX: Use HTTP response caching to avoid calling HTTP API on every GetQuotesEx() call
				// Check if we have a recent HTTP response cached (within last 60 seconds)
				// For real-time updates, rely on WebSocket ticks (processed every 100ms by timer)
				// Only fetch HTTP for initial load or periodic validation (every 60 seconds)

				CString cacheKey;
				cacheKey.Format(_T("%s-%d"), pszTicker, 60);  // "RELIANCE-NSE-60"

				DWORD currentTime = (DWORD)GetTickCount64();
				BOOL bShouldCallHttp = TRUE;  // Default: call HTTP
				int httpLastValid = nQty - 1;  // Start with existing bar count

				// Check cache
				EnterCriticalSection(&g_HttpCacheCriticalSection);
				void* pCacheValue = NULL;
				if (g_HttpResponseCache.Lookup(cacheKey, pCacheValue) && pCacheValue != NULL)
				{
					DWORD lastHttpCallTime = *(DWORD*)pCacheValue;
					DWORD timeSinceLastCall = currentTime - lastHttpCallTime;

					if (timeSinceLastCall < HTTP_CACHE_LIFETIME_MS)
					{
						// Cache is FRESH - SKIP HTTP call, use tick bars only
						bShouldCallHttp = FALSE;

						CString cacheLog;
						cacheLog.Format(_T("OpenAlgo: GetQuotesEx - HTTP cache HIT (%.1f seconds old), skipping HTTP call"),
							timeSinceLastCall / 1000.0f);
						OutputDebugString(cacheLog);
					}
					else
					{
						// Cache is STALE - need to refresh
						CString cacheLog;
						cacheLog.Format(_T("OpenAlgo: GetQuotesEx - HTTP cache STALE (%.1f seconds old), calling HTTP"),
							timeSinceLastCall / 1000.0f);
						OutputDebugString(cacheLog);
					}
				}
				else
				{
					// No cache entry - first call for this symbol
					OutputDebugString(_T("OpenAlgo: GetQuotesEx - HTTP cache MISS (first call), calling HTTP"));
				}
				LeaveCriticalSection(&g_HttpCacheCriticalSection);

				// Call HTTP API if needed
				if (bShouldCallHttp)
				{
					OutputDebugString(_T("OpenAlgo: GetQuotesEx - Fetching HTTP backfill..."));

					// Fetch HTTP backfill data (source of truth for completed bars)
					httpLastValid = GetOpenAlgoHistory(pszTicker, 60, nQty - 1, nSize, pQuotes);

					// Update cache with current time
					EnterCriticalSection(&g_HttpCacheCriticalSection);
					if (pCacheValue != NULL)
					{
						// Update existing cache entry
						*(DWORD*)pCacheValue = currentTime;
					}
					else
					{
						// Create new cache entry
						DWORD* pNewTime = new DWORD;
						*pNewTime = currentTime;
						g_HttpResponseCache.SetAt(cacheKey, pNewTime);
					}
					LeaveCriticalSection(&g_HttpCacheCriticalSection);
				}
				else
				{
					// Skip HTTP call - will use existing bars + tick bars
					httpLastValid = nQty - 1;
				}

				// Only process HTTP response if we actually called HTTP
				int cleanedBarCount = httpLastValid;

				if (bShouldCallHttp)
				{
					CString httpLog;
					httpLog.Format(_T("OpenAlgo: ===== HTTP API RESPONSE ====="));
					OutputDebugString(httpLog);
					httpLog.Format(_T("OpenAlgo: HTTP returned %d bars for %s"), httpLastValid, pszTicker);
					OutputDebugString(httpLog);

					// Log last 3 HTTP bars for debugging
					if (httpLastValid > 0)
					{
						int startIdx = max(0, httpLastValid - 3);
						for (int i = startIdx; i < httpLastValid; i++)
						{
							AmiDate barDate = pQuotes[i].DateTime;
							CString barLog;
							barLog.Format(_T("OpenAlgo: HTTP Bar[%d]: %04d-%02d-%02d %02d:%02d O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f"),
								i, barDate.PackDate.Year, barDate.PackDate.Month, barDate.PackDate.Day,
								barDate.PackDate.Hour, barDate.PackDate.Minute,
								pQuotes[i].Open, pQuotes[i].High, pQuotes[i].Low, pQuotes[i].Price, pQuotes[i].Volume);
							OutputDebugString(barLog);
						}
					}

					// CRITICAL FIX: Remove corrupted bar and duplicate timestamps from HTTP response
					// The HTTP API has TWO bugs:
					// 1. Returns corrupted last bar with invalid timestamp (Hour=31, Minute=63)
					// 2. Keeps adding bars with same timestamp instead of updating
					cleanedBarCount = httpLastValid;

				// STEP 1: Remove corrupted last bar if present
				if (httpLastValid >= 1)
				{
					AmiDate lastBar = pQuotes[httpLastValid - 1].DateTime;

					// Check for invalid timestamp (Hour > 23 or Minute > 59)
					if (lastBar.PackDate.Hour > 23 || lastBar.PackDate.Minute > 59)
					{
						cleanedBarCount = httpLastValid - 1;

						CString corruptLog;
						corruptLog.Format(_T("OpenAlgo: CORRUPTED BAR DETECTED! Removed bar with invalid timestamp %04d-%02d-%02d %02d:%02d"),
							lastBar.PackDate.Year, lastBar.PackDate.Month, lastBar.PackDate.Day,
							lastBar.PackDate.Hour, lastBar.PackDate.Minute);
						OutputDebugString(corruptLog);
					}
				}

				// STEP 2: Remove duplicate timestamps (scan last 50 bars for comprehensive cleanup)
				// Strategy: Scan backwards, keep LAST occurrence of each timestamp (most recent data)
				if (cleanedBarCount >= 2)
				{
					int scanStart = max(0, cleanedBarCount - 50);
					int writeIdx = cleanedBarCount - 1;  // Start from end, write backwards

					// Mark which bars to keep (work backwards to keep last occurrence)
					BOOL* keepBar = new BOOL[cleanedBarCount];
					memset(keepBar, 0, cleanedBarCount * sizeof(BOOL));

					// Always keep the last bar (after removing corrupted bar)
					keepBar[cleanedBarCount - 1] = TRUE;

					// Scan backwards from second-to-last bar
					for (int i = cleanedBarCount - 2; i >= scanStart; i--)
					{
						AmiDate currDate = pQuotes[i].DateTime;
						BOOL isDuplicate = FALSE;

						// Check if this timestamp exists in any LATER bar (already processed)
						for (int j = i + 1; j < cleanedBarCount; j++)
						{
							if (!keepBar[j]) continue;  // Skip bars we're already removing

							AmiDate laterDate = pQuotes[j].DateTime;

							if (currDate.PackDate.Year == laterDate.PackDate.Year &&
								currDate.PackDate.Month == laterDate.PackDate.Month &&
								currDate.PackDate.Day == laterDate.PackDate.Day &&
								currDate.PackDate.Hour == laterDate.PackDate.Hour &&
								currDate.PackDate.Minute == laterDate.PackDate.Minute)
							{
								// Duplicate found - a later bar has same timestamp
								// Keep the LATER bar (more recent data), remove this one
								isDuplicate = TRUE;

								CString dupLog;
								dupLog.Format(_T("OpenAlgo: Removing duplicate bar[%d] at %04d-%02d-%02d %02d:%02d (keeping bar[%d] with newer data)"),
									i, currDate.PackDate.Year, currDate.PackDate.Month, currDate.PackDate.Day,
									currDate.PackDate.Hour, currDate.PackDate.Minute, j);
								OutputDebugString(dupLog);
								break;
							}
						}

						// Keep this bar if it's not a duplicate
						if (!isDuplicate)
						{
							keepBar[i] = TRUE;
						}
					}

					// Compact the array - move kept bars to front
					int newCount = 0;
					for (int i = 0; i < cleanedBarCount; i++)
					{
						if (keepBar[i])
						{
							if (i != newCount)
							{
								pQuotes[newCount] = pQuotes[i];
							}
							newCount++;
						}
					}

					int removedCount = cleanedBarCount - newCount;
					if (removedCount > 0)
					{
						CString summaryLog;
						summaryLog.Format(_T("OpenAlgo: DUPLICATE CLEANUP SUMMARY: Removed %d duplicate bars"), removedCount);
						OutputDebugString(summaryLog);
					}

					cleanedBarCount = newCount;
					delete[] keepBar;
				}

					httpLastValid = cleanedBarCount;

					CString cleanupLog;
					cleanupLog.Format(_T("OpenAlgo: After cleanup: %d bars (removed corrupted + duplicates)"), httpLastValid);
					OutputDebugString(cleanupLog);
				}
				// End of HTTP response processing

				// Log tick bar data for debugging
				if (pBuilder->bBarStarted)
				{
					AmiDate tickDate = pBuilder->currentBar.DateTime;
					CString tickLog;
					tickLog.Format(_T("OpenAlgo: ===== TICK BAR DATA ====="));
					OutputDebugString(tickLog);
					tickLog.Format(_T("OpenAlgo: Tick Bar: %04d-%02d-%02d %02d:%02d O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f TickCnt=%d"),
						tickDate.PackDate.Year, tickDate.PackDate.Month, tickDate.PackDate.Day,
						tickDate.PackDate.Hour, tickDate.PackDate.Minute,
						pBuilder->currentBar.Open, pBuilder->currentBar.High,
						pBuilder->currentBar.Low, pBuilder->currentBar.Price,
						pBuilder->currentBar.Volume, pBuilder->tickCount);
					OutputDebugString(tickLog);
				}

				// CRITICAL FIX: Check if last HTTP bar has same timestamp as tick bar
				// If yes, REPLACE it (don't append) to avoid duplicate timestamps
				if (httpLastValid > 0 && pBuilder->bBarStarted)
				{
					BOOL bReplacedLastBar = FALSE;
					int tickBarIndex = httpLastValid;  // Default: append after HTTP bars

					// Check if last HTTP bar has same timestamp as tick bar
					if (httpLastValid > 0)
					{
						AmiDate lastHttpDate = pQuotes[httpLastValid - 1].DateTime;
						AmiDate tickBarDate = pBuilder->currentBar.DateTime;

						// Compare timestamps (date + time components)
						if (lastHttpDate.PackDate.Year == tickBarDate.PackDate.Year &&
							lastHttpDate.PackDate.Month == tickBarDate.PackDate.Month &&
							lastHttpDate.PackDate.Day == tickBarDate.PackDate.Day &&
							lastHttpDate.PackDate.Hour == tickBarDate.PackDate.Hour &&
							lastHttpDate.PackDate.Minute == tickBarDate.PackDate.Minute)
						{
							// Same timestamp - REPLACE the last HTTP bar with tick bar
							tickBarIndex = httpLastValid - 1;
							bReplacedLastBar = TRUE;
						}
					}

					// Set the tick bar (either replace last or append new)
					if (tickBarIndex < nSize)
					{
						pQuotes[tickBarIndex] = pBuilder->currentBar;
						nQty = bReplacedLastBar ? httpLastValid : (httpLastValid + 1);

						CString mergeLog;
						mergeLog.Format(_T("OpenAlgo: ===== MERGE RESULT ====="));
						OutputDebugString(mergeLog);
						mergeLog.Format(_T("OpenAlgo: %s tick bar at index [%d]"),
							bReplacedLastBar ? _T("REPLACED") : _T("APPENDED"), tickBarIndex);
						OutputDebugString(mergeLog);
						mergeLog.Format(_T("OpenAlgo: Final bar: O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f TickCnt=%d"),
							pBuilder->currentBar.Open, pBuilder->currentBar.High,
							pBuilder->currentBar.Low, pBuilder->currentBar.Price,
							pBuilder->currentBar.Volume, pBuilder->tickCount);
						OutputDebugString(mergeLog);
					}
					else
					{
						nQty = httpLastValid;
						OutputDebugString(_T("OpenAlgo: GetQuotesEx - No space for tick bar"));
					}
				}
				else
				{
					nQty = httpLastValid;
					OutputDebugString(_T("OpenAlgo: GetQuotesEx - No tick bar to append (bar not started or no space)"));
				}

				CString finalLog;
				finalLog.Format(_T("OpenAlgo: ===== FINAL RESULT ====="));
				OutputDebugString(finalLog);
				finalLog.Format(_T("OpenAlgo: Returning %d total bars (HTTP + tick) for %s"), nQty, pszTicker);
				OutputDebugString(finalLog);

				LeaveCriticalSection(&g_BarBuilderCriticalSection);
			}
			else
			{
				OutputDebugString(_T("OpenAlgo: GetQuotesEx - No BarBuilder found, using pure HTTP backfill"));

				// No BarBuilder yet - use pure HTTP backfill
				nQty = GetOpenAlgoHistory(pszTicker, 60, nQty - 1, nSize, pQuotes);

				CString httpLog;
				httpLog.Format(_T("OpenAlgo: GetQuotesEx - Pure HTTP returned %d bars"), nQty);
				OutputDebugString(httpLog);
			}
		}
		else
		{
			// Real-time disabled - use pure HTTP backfill (original behavior)
			nQty = GetOpenAlgoHistory(pszTicker, 60, nQty - 1, nSize, pQuotes);
		}

		return nQty;
	}
	else
	{
		// Unsupported interval - return existing data
		return nLastValid + 1;
	}
}

// GetRecentInfo is ONLY for Real-time Quote Window display
// This function provides Level 1 quotes for the quote window
// It should NEVER be used for chart data or OHLC bars
PLUGINAPI struct RecentInfo* GetRecentInfo(LPCTSTR pszTicker)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	// Check if we're connected and have an API key
	if (g_nStatus != STATUS_CONNECTED || g_oApiKey.IsEmpty())
		return NULL;

	static struct RecentInfo ri;
	memset(&ri, 0, sizeof(ri));
	ri.nStructSize = sizeof(struct RecentInfo);

	CString ticker(pszTicker);
	
	// Initialize WebSocket connection if needed (but don't block if connection is in progress)
	// Also add a delay between connection attempts to avoid hammering the server
	DWORD dwNow = (DWORD)GetTickCount64();
	if (!g_bWebSocketConnected && !g_bWebSocketConnecting && 
		(dwNow - g_dwLastConnectionAttempt) > 10000) // Wait 10 seconds between attempts
	{
		g_dwLastConnectionAttempt = dwNow;
		InitializeWebSocket();
	}

	// Critical section should already be initialized in Init()

	// Check if this symbol is already subscribed via WebSocket
	BOOL bSubscribed = FALSE;
	EnterCriticalSection(&g_WebSocketCriticalSection);
	
	if (!g_SubscribedSymbols.Lookup(ticker, bSubscribed))
	{
		// Symbol not subscribed yet, subscribe to it
		if (g_bWebSocketConnected)
		{
			// Give authentication a moment to complete if it's still processing
			if (!g_bWebSocketAuthenticated)
			{
				Sleep(100);
			}
			
			// Try to subscribe - authentication will be handled automatically
			if (SubscribeToSymbol(pszTicker))
			{
				g_SubscribedSymbols.SetAt(ticker, TRUE);
				
				// Mark as authenticated since we successfully sent a subscribe request
				// (this handles cases where auth response parsing failed but server accepted subscription)
				if (!g_bWebSocketAuthenticated)
				{
					g_bWebSocketAuthenticated = TRUE;
				}
			}
		}
	}
	
	LeaveCriticalSection(&g_WebSocketCriticalSection);

	// Process any pending WebSocket data
	ProcessWebSocketData();

	// Check cache for WebSocket data first
	QuoteCache cachedQuote;
	BOOL bCached = FALSE;

	if (g_QuoteCache.Lookup(ticker, cachedQuote))
	{
		// Use cached data if it's less than 5 seconds old
		DWORD dwNow = (DWORD)GetTickCount64();
		if ((dwNow - cachedQuote.lastUpdate) < 5000)
		{
			bCached = TRUE;
		}
	}

	// Fallback to HTTP API if WebSocket data not available
	if (!bCached)
	{
		if (!GetOpenAlgoQuote(pszTicker, cachedQuote))
			return NULL;

		// Store in cache
		g_QuoteCache.SetAt(ticker, cachedQuote);
	}

	// Fill RecentInfo structure
	_tcsncpy_s(ri.Name, sizeof(ri.Name) / sizeof(TCHAR), pszTicker, _TRUNCATE);
	_tcsncpy_s(ri.Exchange, sizeof(ri.Exchange) / sizeof(TCHAR), cachedQuote.exchange, _TRUNCATE);

	ri.nStatus = RI_STATUS_UPDATE | RI_STATUS_TRADE | RI_STATUS_BARSREADY;
	ri.nBitmap = RI_LAST | RI_OPEN | RI_HIGHLOW | RI_TRADEVOL | RI_OPENINT;

	ri.fLast = cachedQuote.ltp;
	ri.fOpen = cachedQuote.open;
	ri.fHigh = cachedQuote.high;
	ri.fLow = cachedQuote.low;
	ri.fPrev = cachedQuote.close;
	ri.fChange = cachedQuote.ltp - cachedQuote.close;
	ri.fTradeVol = cachedQuote.volume;
	ri.fTotalVol = cachedQuote.volume;
	ri.fOpenInt = cachedQuote.oi;

	// Set update times
	CTime now = CTime::GetCurrentTime();
	ri.nDateUpdate = now.GetYear() * 10000 + now.GetMonth() * 100 + now.GetDay();
	ri.nTimeUpdate = now.GetHour() * 10000 + now.GetMinute() * 100 + now.GetSecond();
	ri.nDateChange = ri.nDateUpdate;
	ri.nTimeChange = ri.nTimeUpdate;

	return &ri;
}

///////////////////////////////
// WebSocket Functions
///////////////////////////////

void GenerateWebSocketMaskKey(unsigned char* maskKey)
{
	// Generate a simple random mask key
	srand((unsigned int)GetTickCount64());
	maskKey[0] = (unsigned char)(rand() & 0xFF);
	maskKey[1] = (unsigned char)(rand() & 0xFF);
	maskKey[2] = (unsigned char)(rand() & 0xFF);
	maskKey[3] = (unsigned char)(rand() & 0xFF);
}

BOOL SendWebSocketFrame(const CString& message)
{
	if (g_websocket == INVALID_SOCKET)
		return FALSE;

	// Convert message to UTF-8
	CStringA messageA(message);
	int messageLen = messageA.GetLength();
	
	// Create WebSocket frame
	unsigned char frame[1024];
	int frameLen = 0;
	
	// First byte: FIN=1, OpCode=1 (text frame)
	frame[frameLen++] = 0x81;
	
	// Second byte: MASK=1 + Payload length
	if (messageLen < 126)
	{
		frame[frameLen++] = 0x80 | messageLen;
	}
	else if (messageLen < 65536)
	{
		frame[frameLen++] = 0x80 | 126;
		frame[frameLen++] = (messageLen >> 8) & 0xFF;
		frame[frameLen++] = messageLen & 0xFF;
	}
	else
	{
		return FALSE; // Message too long
	}
	
	// Generate masking key
	unsigned char maskKey[4];
	GenerateWebSocketMaskKey(maskKey);
	memcpy(&frame[frameLen], maskKey, 4);
	frameLen += 4;
	
	// Masked payload
	for (int i = 0; i < messageLen; i++)
	{
		frame[frameLen++] = messageA[i] ^ maskKey[i % 4];
	}
	
	// Send the frame
	int sent = send(g_websocket, (char*)frame, frameLen, 0);
	return (sent == frameLen);
}

CString DecodeWebSocketFrame(const char* buffer, int length)
{
	CString result;
	
	if (length < 2) return result;
	
	int pos = 0;
	unsigned char firstByte = (unsigned char)buffer[pos++];
	unsigned char secondByte = (unsigned char)buffer[pos++];
	
	// Check frame type
	unsigned char opcode = firstByte & 0x0F;
	
	// Extract payload info (needed for all frame types)
	BOOL masked = (secondByte & 0x80) != 0;
	int payloadLen = secondByte & 0x7F;

	// Handle different frame types
	if (opcode == 0x08) // Close frame
	{
		// Extract close status code and reason from payload
		CString closeInfo = _T("CLOSE_FRAME");

		// Handle extended payload length for close frames
		int closePos = pos;
		if (payloadLen == 126)
		{
			if (closePos + 2 > length) return closeInfo;
			payloadLen = ((unsigned char)buffer[closePos] << 8) | (unsigned char)buffer[closePos + 1];
			closePos += 2;
		}

		// Extract masking key if present
		unsigned char closeMaskKey[4] = {0};
		if (masked)
		{
			if (closePos + 4 > length) return closeInfo;
			memcpy(closeMaskKey, &buffer[closePos], 4);
			closePos += 4;
		}

		// Extract status code (first 2 bytes of payload)
		if (payloadLen >= 2 && closePos + 2 <= length)
		{
			unsigned char byte1 = masked ? (buffer[closePos] ^ closeMaskKey[0]) : buffer[closePos];
			unsigned char byte2 = masked ? (buffer[closePos + 1] ^ closeMaskKey[1]) : buffer[closePos + 1];
			int statusCode = (byte1 << 8) | byte2;

			// Extract reason text if present
			CString reason;
			if (payloadLen > 2 && closePos + payloadLen <= length)
			{
				int reasonLen = payloadLen - 2;
				CStringA reasonA;
				char* reasonBuf = reasonA.GetBuffer(reasonLen + 1);
				
				for (int i = 0; i < reasonLen; i++)
				{
					reasonBuf[i] = masked ? (buffer[closePos + 2 + i] ^ closeMaskKey[(i + 2) % 4]) : buffer[closePos + 2 + i];
				}
				reasonBuf[reasonLen] = '\0';
				reasonA.ReleaseBuffer(reasonLen);
				reason = CString(reasonA);
			}

			if (reason.IsEmpty())
			{
				closeInfo.Format(_T("CLOSE_FRAME (Status Code: %d)"), statusCode);
			}
			else
			{
				closeInfo.Format(_T("CLOSE_FRAME (Status Code: %d, Reason: %s)"), statusCode, reason);
			}
		}

		return closeInfo;
	}
	else if (opcode == 0x09) // Ping frame
	{
		// Extract PING payload to echo back in PONG (RFC 6455 Section 5.5.3 requirement)
		CString pingResult = _T("PING_FRAME");

		// Handle extended payload length
		int pingPos = pos;
		if (payloadLen == 126)
		{
			if (pingPos + 2 > length) return pingResult;
			payloadLen = ((unsigned char)buffer[pingPos] << 8) | (unsigned char)buffer[pingPos + 1];
			pingPos += 2;
		}

		// Extract masking key if present
		unsigned char pingMaskKey[4] = {0};
		if (masked)
		{
			if (pingPos + 4 > length) return pingResult;
			memcpy(pingMaskKey, &buffer[pingPos], 4);
			pingPos += 4;
		}

		// Extract payload (usually 4 bytes from Python websockets library)
		if (payloadLen > 0 && payloadLen <= 125 && pingPos + payloadLen <= length)
		{
			// OPTIMIZED: Direct hex writing to avoid excessive allocations
			CStringA hexPayload;
			char* hexBuf = hexPayload.GetBuffer(payloadLen * 2 + 1);
			const char* hexChars = "0123456789ABCDEF";
			
			for (int i = 0; i < payloadLen; i++)
			{
				unsigned char byte = masked ? (buffer[pingPos + i] ^ pingMaskKey[i % 4]) : buffer[pingPos + i];
				hexBuf[i * 2] = hexChars[(byte >> 4) & 0xF];
				hexBuf[i * 2 + 1] = hexChars[byte & 0xF];
			}
			hexBuf[payloadLen * 2] = '\0';
			hexPayload.ReleaseBuffer(payloadLen * 2);

			// Return "PING_FRAME:XXXXXXXX" where X is hex-encoded payload
			pingResult = _T("PING_FRAME:") + CString(hexPayload);
		}

		return pingResult;
	}
	else if (opcode == 0x0A) // Pong frame
	{
		return _T("PONG_FRAME");
	}
	else if (opcode != 0x01) // Not a text frame
	{
		return result;
	}
	
	// Handle extended payload length
	if (payloadLen == 126)
	{
		if (pos + 2 > length) return result;
		payloadLen = ((unsigned char)buffer[pos] << 8) | (unsigned char)buffer[pos + 1];
		pos += 2;
	}
	else if (payloadLen == 127)
	{
		return result; // 64-bit length not supported
	}
	
	// Validate payload length doesn't exceed buffer
	if (payloadLen <= 0 || payloadLen > 16000) return result;
	
	// Handle masking key
	unsigned char maskKey[4] = {0};
	if (masked)
	{
		if (pos + 4 > length) return result;
		memcpy(maskKey, &buffer[pos], 4);
		pos += 4;
	}
	
	// Validate we have enough data for the payload
	if (pos + payloadLen > length) return result;
	
	// Extract and unmask payload
	// OPTIMIZED: Use CStringA::GetBuffer to avoid extra copy
	CStringA payloadA;
	char* payloadBuffer = payloadA.GetBuffer(payloadLen + 1);
	
	// If masked, we must unmask. If not masked, we can use memcpy for speed
	if (masked)
	{
		for (int i = 0; i < payloadLen; i++)
		{
			payloadBuffer[i] = buffer[pos + i] ^ maskKey[i % 4];
		}
	}
	else
	{
		memcpy(payloadBuffer, &buffer[pos], payloadLen);
	}
	
	payloadBuffer[payloadLen] = '\0';
	payloadA.ReleaseBuffer(payloadLen);
	
	result = CString(payloadA);
	
	return result;
}

BOOL InitializeWebSocket(void)
{
	OutputDebugString(_T("OpenAlgo: InitializeWebSocket() called"));

	if (g_bWebSocketConnected)
	{
		OutputDebugString(_T("OpenAlgo: InitializeWebSocket - Already connected, returning TRUE"));
		return TRUE;
	}

	if (g_bWebSocketConnecting)
	{
		OutputDebugString(_T("OpenAlgo: InitializeWebSocket - Connection in progress, returning FALSE"));
		return FALSE; // Connection in progress, don't start another
	}

	if (g_oWebSocketUrl.IsEmpty() || g_oApiKey.IsEmpty())
	{
		CString errMsg;
		errMsg.Format(_T("OpenAlgo: InitializeWebSocket - FAILED: URL='%s' APIKey='%s'"),
			g_oWebSocketUrl.IsEmpty() ? _T("EMPTY") : g_oWebSocketUrl,
			g_oApiKey.IsEmpty() ? _T("EMPTY") : _T("SET"));
		OutputDebugString(errMsg);
		return FALSE;
	}

	CString startMsg;
	startMsg.Format(_T("OpenAlgo: InitializeWebSocket - Starting connection to %s"), g_oWebSocketUrl);
	OutputDebugString(startMsg);

	// Initialize Winsock
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
	{
		OutputDebugString(_T("OpenAlgo: InitializeWebSocket - WSAStartup FAILED"));
		return FALSE;
	}

	OutputDebugString(_T("OpenAlgo: InitializeWebSocket - WSAStartup succeeded, calling ConnectWebSocket()"));

	g_bWebSocketConnecting = TRUE;
	BOOL result = ConnectWebSocket();
	g_bWebSocketConnecting = FALSE;

	CString resultMsg;
	resultMsg.Format(_T("OpenAlgo: InitializeWebSocket - ConnectWebSocket returned %d"), result);
	OutputDebugString(resultMsg);

	return result;
}

BOOL ConnectWebSocket(void)
{
	// Parse WebSocket URL
	CString host, path;
	int port = 80;
	
	CString url = g_oWebSocketUrl;
	if (url.Left(5) == _T("wss://"))
	{
		port = 443;
		url = url.Mid(6);
	}
	else if (url.Left(5) == _T("ws://"))
	{
		url = url.Mid(5);
	}
	
	// Extract host and port
	int slashPos = url.Find(_T('/'));
	if (slashPos > 0)
	{
		host = url.Left(slashPos);
		path = url.Mid(slashPos);
	}
	else
	{
		host = url;
		path = _T("/");
	}
	
	int colonPos = host.Find(_T(':'));
	if (colonPos > 0)
	{
		CString portStr = host.Mid(colonPos + 1);
		port = _ttoi(portStr);
		host = host.Left(colonPos);
	}
	
	// Create socket
	g_websocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (g_websocket == INVALID_SOCKET)
		return FALSE;
	
	// Set socket timeouts (blocking mode initially)
	int timeout = 5000; // 5 seconds
	setsockopt(g_websocket, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
	setsockopt(g_websocket, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout));
	
	// Resolve hostname
	struct addrinfo hints, *result;
	ZeroMemory(&hints, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	
	CStringA hostA(host);
	CStringA portStrA;
	portStrA.Format("%d", port);
	
	if (getaddrinfo(hostA, portStrA, &hints, &result) != 0)
	{
		closesocket(g_websocket);
		g_websocket = INVALID_SOCKET;
		return FALSE;
	}
	
	// Connect to server (blocking with timeout)
	if (connect(g_websocket, result->ai_addr, (int)result->ai_addrlen) == SOCKET_ERROR)
	{
		freeaddrinfo(result);
		closesocket(g_websocket);
		g_websocket = INVALID_SOCKET;
		return FALSE;
	}
	
	freeaddrinfo(result);
	
	// Send WebSocket upgrade request
	CString upgradeRequest;
	upgradeRequest.Format(
		_T("GET %s HTTP/1.1\r\n")
		_T("Host: %s:%d\r\n")
		_T("Upgrade: websocket\r\n")
		_T("Connection: Upgrade\r\n")
		_T("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
		_T("Sec-WebSocket-Version: 13\r\n")
		_T("\r\n"),
		(LPCTSTR)path, (LPCTSTR)host, port);
	
	CStringA requestA(upgradeRequest);
	if (send(g_websocket, requestA, requestA.GetLength(), 0) == SOCKET_ERROR)
	{
		closesocket(g_websocket);
		g_websocket = INVALID_SOCKET;
		return FALSE;
	}
	
	// Wait for upgrade response with proper timeout
	char buffer[1024];
	int received = recv(g_websocket, buffer, sizeof(buffer) - 1, 0);
	if (received > 0)
	{
		buffer[received] = '\0';
		CString response(buffer);
		
		if (response.Find(_T("101")) > 0 && response.Find(_T("Switching Protocols")) > 0)
		{
			g_bWebSocketConnected = TRUE;
			
			// Small delay to allow WebSocket connection to stabilize
			Sleep(200);
			
			// Now set to non-blocking mode for ongoing operations
			u_long mode = 1;
			ioctlsocket(g_websocket, FIONBIO, &mode);
			
			// Authenticate after switching to non-blocking mode
			return AuthenticateWebSocket();
		}
	}
	
	closesocket(g_websocket);
	g_websocket = INVALID_SOCKET;
	return FALSE;
}

BOOL AuthenticateWebSocket(void)
{
	OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket() called"));

	if (!g_bWebSocketConnected)
	{
		OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - WebSocket NOT CONNECTED, cannot authenticate"));
		return FALSE;
	}

	// Send authentication message
	CString authMsg = _T("{\"action\":\"authenticate\",\"api_key\":\"") + g_oApiKey + _T("\"}");

	// Log authentication message (mask API key for security)
	CString logMsg;
	if (g_oApiKey.GetLength() > 4)
	{
		logMsg.Format(_T("OpenAlgo: AuthenticateWebSocket - Sending auth with API key: %s...%s"),
			g_oApiKey.Left(2), g_oApiKey.Right(2));
	}
	else
	{
		logMsg = _T("OpenAlgo: AuthenticateWebSocket - Sending auth (API key too short or empty!)");
	}
	OutputDebugString(logMsg);

	if (SendWebSocketFrame(authMsg))
	{
		OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - Sent auth message, waiting for response..."));
		// Wait for authentication response with select (for non-blocking socket)
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(g_websocket, &readfds);
		
		struct timeval timeout;
		timeout.tv_sec = 5;  // Increased timeout to 5 seconds
		timeout.tv_usec = 0;
		
		if (select(0, &readfds, NULL, NULL, &timeout) > 0)
		{
			OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - Response received from server"));
			char authBuffer[1024];
			int received = recv(g_websocket, authBuffer, sizeof(authBuffer) - 1, 0);

			CString recvLog;
			recvLog.Format(_T("OpenAlgo: AuthenticateWebSocket - recv() returned %d bytes"), received);
			OutputDebugString(recvLog);

			if (received > 0)
			{
				authBuffer[received] = '\0';
				CString authResponse = DecodeWebSocketFrame(authBuffer, received);

				CString respLog;
				respLog.Format(_T("OpenAlgo: AuthenticateWebSocket - Decoded response: %s"), authResponse);
				OutputDebugString(respLog);

				// Check for success status in authentication response
				// Look for various success indicators that OpenAlgo might send
				if (authResponse.Find(_T("success")) >= 0 ||
					authResponse.Find(_T("authenticated")) >= 0 ||
					authResponse.Find(_T("\"status\":\"ok\"")) >= 0 ||
					authResponse.Find(_T("\"status\":\"success\"")) >= 0)
				{
					OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - Authentication SUCCESSFUL!"));
					g_bWebSocketAuthenticated = TRUE;
					// Small delay to ensure server has processed authentication
					Sleep(200);

					// Trigger any pending subscriptions now that we're authenticated
					SubscribePendingSymbols();
					return TRUE;
				}
				else if (authResponse.Find(_T("error")) >= 0 || authResponse.Find(_T("failed")) >= 0)
				{
					// Explicit authentication failure
					OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - Authentication FAILED (error in response)"));
					OutputDebugString(authResponse);
					return FALSE;
				}
				else
				{
					OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - Received response but no clear success/failure indicator"));
				}
			}
			else if (received == 0)
			{
				OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - SERVER CLOSED CONNECTION during auth!"));
				g_bWebSocketConnected = FALSE;
				return FALSE;
			}
		}
		else
		{
			OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - No response within 5 seconds (timeout)"));
		}

		// If we reach here, either timeout or no clear response
		// Since authentication was sent successfully, assume success
		// This is a fallback since the test button works with the same flow
		OutputDebugString(_T("OpenAlgo: AuthenticateWebSocket - Assuming authentication succeeded (fallback)"));
		g_bWebSocketAuthenticated = TRUE;
		// Longer delay to ensure server has processed authentication
		Sleep(1000);  // Increased delay to ensure server processes auth

		// Trigger any pending subscriptions now that we're authenticated
		SubscribePendingSymbols();
		return TRUE;
	}
	
	return FALSE;
}

BOOL SubscribeToSymbol(LPCTSTR pszTicker)
{
	CString logStart;
	logStart.Format(_T("OpenAlgo: SubscribeToSymbol called for: %s"), pszTicker);
	OutputDebugString(logStart);

	if (!g_bWebSocketConnected)
	{
		OutputDebugString(_T("OpenAlgo: SubscribeToSymbol - WebSocket NOT CONNECTED, cannot subscribe"));
		return FALSE;
	}

	// Extract symbol and exchange
	CString symbol = GetCleanSymbol(pszTicker);
	CString exchange = GetExchangeFromTicker(pszTicker);

	CString extractLog;
	extractLog.Format(_T("OpenAlgo: SubscribeToSymbol - Extracted: Symbol='%s' Exchange='%s'"),
		symbol, exchange);
	OutputDebugString(extractLog);

	// Send subscription message for quote mode (mode 2)
	// For quotes WebSocket (not market depth), no depth field needed
	CString subMsg;
	subMsg.Format(_T("{\"action\":\"subscribe\",\"symbol\":\"%s\",\"exchange\":\"%s\",\"mode\":2}"),
		(LPCTSTR)symbol, (LPCTSTR)exchange);

	CString msgLog;
	msgLog.Format(_T("OpenAlgo: SubscribeToSymbol - Sending: %s"), subMsg);
	OutputDebugString(msgLog);

	BOOL result = SendWebSocketFrame(subMsg);

	CString resultLog;
	resultLog.Format(_T("OpenAlgo: SubscribeToSymbol - SendWebSocketFrame returned %d"), result);
	OutputDebugString(resultLog);

	return result;
}

BOOL UnsubscribeFromSymbol(LPCTSTR pszTicker)
{
	if (!g_bWebSocketConnected)
		return FALSE;
	
	// Extract symbol and exchange
	CString symbol = GetCleanSymbol(pszTicker);
	CString exchange = GetExchangeFromTicker(pszTicker);
	
	// Send unsubscription message
	CString unsubMsg;
	unsubMsg.Format(_T("{\"action\":\"unsubscribe\",\"symbol\":\"%s\",\"exchange\":\"%s\",\"mode\":2}"),
		(LPCTSTR)symbol, (LPCTSTR)exchange);
	
	return SendWebSocketFrame(unsubMsg);
}

void SubscribePendingSymbols(void)
{
	// This function subscribes to symbols that are in the real-time quote window
	// but haven't been subscribed to WebSocket yet
	
	if (!g_bWebSocketConnected || !g_bWebSocketAuthenticated)
		return;
	
	EnterCriticalSection(&g_WebSocketCriticalSection);
	
	// Since we don't have a list of symbols from the real-time quote window directly,
	// we'll rely on GetRecentInfo being called for active symbols to trigger subscriptions
	// For now, just ensure we're ready to handle subscriptions
	
	LeaveCriticalSection(&g_WebSocketCriticalSection);
}

BOOL ProcessWebSocketData(void)
{
	// LOGGING CONTROL: Only log basic connection stats occasionally
	static int s_callCount = 0;
	s_callCount++;
	if (s_callCount % 1000 == 0)  // Log every 1000th call (approx every 1 sec at 1ms poll)
	{
		// Reduced logging frequency
		/*
		CString callMsg;
		callMsg.Format(_T("OpenAlgo: ProcessWebSocketData() call #%d - Connected=%d Socket=%d"),
			s_callCount, g_bWebSocketConnected, (g_websocket != INVALID_SOCKET));
		OutputDebugString(callMsg);
		*/
	}

	if (!g_bWebSocketConnected || g_websocket == INVALID_SOCKET)
	{
		// Auto-reconnect if disconnected
		static DWORD lastReconnectAttempt = 0;
		DWORD now = (DWORD)GetTickCount64();

		// Try reconnecting every 5 seconds
		if ((now - lastReconnectAttempt) > 5000)
		{
			lastReconnectAttempt = now;
			OutputDebugString(_T("OpenAlgo: ProcessWebSocketData() - NOT CONNECTED, attempting reconnect..."));

			if (InitializeWebSocket())
			{
				OutputDebugString(_T("OpenAlgo: *** AUTO-RECONNECT SUCCESSFUL! ***"));
				return TRUE; // Continue processing
			}
			else
			{
				OutputDebugString(_T("OpenAlgo: Auto-reconnect failed, will retry in 5 seconds"));
			}
		}
		return FALSE;
	}

	// Send periodic ping to keep connection alive
	static DWORD lastPingTime = 0;
	static BOOL pingTimerInitialized = FALSE;
	DWORD currentTime = (DWORD)GetTickCount64();

	// Initialize timer on first call (don't send PING immediately)
	if (!pingTimerInitialized)
	{
		lastPingTime = currentTime;
		pingTimerInitialized = TRUE;
	}

	if ((currentTime - lastPingTime) > 30000) // Ping every 30 seconds
	{
		// Send WebSocket ping frame (opcode 0x09)
		unsigned char pingFrame[6] = {0x89, 0x84, 0x00, 0x00, 0x00, 0x00}; // Ping with 4-byte mask
		GenerateWebSocketMaskKey(&pingFrame[2]);
		send(g_websocket, (char*)pingFrame, 6, 0);
		lastPingTime = currentTime;
		// Reduced logging: OutputDebugString(_T("OpenAlgo: Sent WebSocket ping"));
	}
	
	// CRITICAL: Read ALL pending data in a loop
	// The server sends subscription ACKs and market data continuously
	// We must drain the receive buffer or the server will close the connection!
	int messagesProcessed = 0;
	const int MAX_MESSAGES_PER_CALL = 200; // Increased to 200 to handle high volume

	while (messagesProcessed < MAX_MESSAGES_PER_CALL)
	{
		// Check for incoming data (non-blocking)
		fd_set readfds;
		FD_ZERO(&readfds);
		FD_SET(g_websocket, &readfds);

		struct timeval timeout;
		timeout.tv_sec = 0;
		timeout.tv_usec = 0; // Non-blocking

		int selectResult = select(0, &readfds, NULL, NULL, &timeout);

		if (selectResult <= 0)
		{
			// No more data available
			break;
		}

		messagesProcessed++;

		if (selectResult > 0)
		{
			// Increased buffer size to 64KB for high throughput
			char buffer[65536]; 
			int received = recv(g_websocket, buffer, sizeof(buffer) - 1, 0);

			// Reduced logging: Don't log every recv() byte count
			
			if (received > 0)
			{
				CString data = DecodeWebSocketFrame(buffer, received);

				if (data.IsEmpty()) continue;

				// Fast path check for control frames
				TCHAR firstChar = data[0];
				if (firstChar == 'P') // PING_FRAME or PONG_FRAME
				{
					if (data == _T("PONG_FRAME"))
					{
						continue; 
					}
					else if (data.Find(_T("PING_FRAME")) == 0) // Starts with "PING_FRAME"
					{
						// Handle PING (Echo back payload)
						CString payload;
						int colonPos = data.Find(':');
						if (colonPos > 0)
						{
							payload = data.Mid(colonPos + 1);
						}

						// Convert hex payload back to bytes
						int payloadLen = payload.GetLength() / 2;
						unsigned char payloadBytes[125] = {0};
						const TCHAR* pPayload = payload;
						for (int i = 0; i < payloadLen && i < 125; i++)
						{
							TCHAR hexByte[3];
							hexByte[0] = pPayload[i * 2];
							hexByte[1] = pPayload[i * 2 + 1];
							hexByte[2] = 0;
							payloadBytes[i] = (unsigned char)_tcstoul(hexByte, NULL, 16);
						}

						// Build PONG frame
						unsigned char pongFrame[256];
						int frameLen = 0;
						pongFrame[frameLen++] = 0x8A;  // FIN + opcode 0x0A (PONG)
						pongFrame[frameLen++] = 0x80 | payloadLen;  // MASK + len

						unsigned char maskKey[4];
						GenerateWebSocketMaskKey(maskKey);
						memcpy(&pongFrame[frameLen], maskKey, 4);
						frameLen += 4;

						for (int i = 0; i < payloadLen; i++)
						{
							pongFrame[frameLen++] = payloadBytes[i] ^ maskKey[i % 4];
						}

						send(g_websocket, (char*)pongFrame, frameLen, 0);
						continue;
					}
				}
				else if (firstChar == 'C' && data.Find(_T("CLOSE_FRAME")) == 0)
				{
					// Connection closed
					OutputDebugString(_T("OpenAlgo: Server sent CLOSE_FRAME"));
					g_bWebSocketConnected = FALSE;
					g_bWebSocketAuthenticated = FALSE;
					closesocket(g_websocket);
					g_websocket = INVALID_SOCKET;
					break;
				}

				// Simple Optimization: Use pointer arithmetic for numeric parsing
				auto ExtractFloat = [&](LPCTSTR searchStr) -> float {
					int pos = data.Find(searchStr);
					if (pos >= 0)
					{
						return (float)_tcstod((LPCTSTR)data + pos + _tcslen(searchStr), NULL);
					}
					return 0.0f;
				};

				// "market_data" check
				if (data.Find(_T("market_data")) >= 0)
				{
					// Extract String fields (Still using Find/Mid for strings as they need termination/copy)
					CString symbol, exchange;
					
					// Symbol
					int symbolPos = data.Find(_T("\"symbol\":"));
					if (symbolPos >= 0)
					{
						symbolPos += 9;
						while (symbolPos < data.GetLength() && (data[symbolPos] == ' ' || data[symbolPos] == '\t')) symbolPos++;
						if (data[symbolPos] == '\"') symbolPos++; // Skip quote
						int endPos = data.Find(_T("\""), symbolPos);
						if (endPos > symbolPos) symbol = data.Mid(symbolPos, endPos - symbolPos);
					}

					// Exchange
					int exchangePos = data.Find(_T("\"exchange\":"));
					if (exchangePos >= 0)
					{
						exchangePos += 11;
						// Fast skip of quote
						while (exchangePos < data.GetLength() && (data[exchangePos] != '\"')) exchangePos++; // Find start quote
						exchangePos++; // Skip start quote
						int endPos = data.Find(_T("\""), exchangePos);
						if (endPos > exchangePos) exchange = data.Mid(exchangePos, endPos - exchangePos);
					}

					// Validate mandatory fields
					if (symbol.IsEmpty() || exchange.IsEmpty()) continue;

					// Extract Numeric Fields (Optimized)
					// _tcstod will stop at ',' or '}' automatically
					float ltp = ExtractFloat(_T("\"ltp\":"));
					float lastTradeQty = ExtractFloat(_T("\"last_trade_quantity\":"));
					
					// Other fields
					float open = ExtractFloat(_T("\"open\":"));
					float high = ExtractFloat(_T("\"high\":"));
					float low = ExtractFloat(_T("\"low\":"));
					float close = ExtractFloat(_T("\"close\":"));
					float volume = ExtractFloat(_T("\"volume\":"));
					float oi = ExtractFloat(_T("\"oi\":"));

					// Timestamp handling
					time_t tickTimestamp = time(NULL); // Default to system time
					int tsPos = data.Find(_T("\"timestamp\":"));
					CString timestampStr;
					if (tsPos >= 0)
					{
						tsPos += 12; // Skip "timestamp":
						int endPos = -1;
						
						// Check if string or number
						if (data[tsPos] == '\"') 
						{
						    // String timestamp
							tsPos++; 
							endPos = data.Find(_T("\""), tsPos);
							if (endPos > tsPos) timestampStr = data.Mid(tsPos, endPos - tsPos);
						}
						else 
						{
						    // Numeric timestamp
							endPos = data.Find(_T(","), tsPos);
							if (endPos < 0) endPos = data.Find(_T("}"), tsPos);
							if (endPos > tsPos) timestampStr = data.Mid(tsPos, endPos - tsPos);
						}
					}
					// (Parsing logic for timestamp remains same as original if needed, or simplifed)

					// Update cache
					{
						QuoteCache quote;
						quote.symbol = symbol;
						quote.exchange = exchange;
						quote.ltp = ltp;
						quote.open = open;
						quote.high = high;
						quote.low = low;
						quote.close = close;
						quote.volume = volume;
						quote.oi = oi;
						quote.lastUpdate = (DWORD)GetTickCount64();

						CString ticker = symbol + _T("-") + exchange;
						g_QuoteCache.SetAt(ticker, quote);

						if (g_bRealTimeCandlesEnabled && ltp > 0)
						{
							if (lastTradeQty <= 0) lastTradeQty = 1.0f;
							
							// Process tick (Logging removed for speed)
							ProcessTick(symbol, exchange, ltp, lastTradeQty, tickTimestamp);
						}
					}
				}
				else if (data.Find(_T("\"type\":\"subscribe\"")) >= 0)
				{
					// Ack - no action needed
				}
			}
			else if (received == 0)
			{
				OutputDebugString(_T("OpenAlgo: Server closed connection (graceful)"));
				g_bWebSocketConnected = FALSE;
				g_bWebSocketAuthenticated = FALSE;
				closesocket(g_websocket);
				g_websocket = INVALID_SOCKET;
				
				EnterCriticalSection(&g_WebSocketCriticalSection);
				g_SubscribedSymbols.RemoveAll();
				LeaveCriticalSection(&g_WebSocketCriticalSection);

				// Attempt immediate reconnect
				InitializeWebSocket();
				break;
			}
		}
	}

	return (messagesProcessed > 0);
}

void CleanupWebSocket(void)
{
	if (g_bCriticalSectionInitialized)
	{
		EnterCriticalSection(&g_WebSocketCriticalSection);
		
		// Unsubscribe from all symbols
		POSITION pos = g_SubscribedSymbols.GetStartPosition();
		while (pos != NULL)
		{
			CString symbol;
			BOOL subscribed;
			g_SubscribedSymbols.GetNextAssoc(pos, symbol, subscribed);
			
			if (subscribed)
			{
				UnsubscribeFromSymbol(symbol);
			}
		}
		
		g_SubscribedSymbols.RemoveAll();
		
		LeaveCriticalSection(&g_WebSocketCriticalSection);
		DeleteCriticalSection(&g_WebSocketCriticalSection);
		g_bCriticalSectionInitialized = FALSE;
	}
	
	// Close WebSocket connection
	if (g_websocket != INVALID_SOCKET)
	{
		closesocket(g_websocket);
		g_websocket = INVALID_SOCKET;
	}
	
	g_bWebSocketConnected = FALSE;
	g_bWebSocketAuthenticated = FALSE;
	g_bWebSocketConnecting = FALSE;
	
	WSACleanup();
}

///////////////////////////////
// Real-Time Candle Building Functions
///////////////////////////////

// Parse ISO 8601 timestamp (e.g., "2025-05-28T10:30:45.123Z")
time_t ParseISO8601Timestamp(const CString& isoTimestamp)
{
	if (isoTimestamp.IsEmpty())
		return time(NULL);

	// Check if it's a Unix timestamp (all digits, possibly with whitespace)
	// Example: "1761157800000" (milliseconds since epoch)
	CString trimmed = isoTimestamp;
	trimmed.Trim();

	BOOL isNumeric = TRUE;
	for (int i = 0; i < trimmed.GetLength(); i++)
	{
		if (!_istdigit(trimmed[i]))
		{
			isNumeric = FALSE;
			break;
		}
	}

	if (isNumeric)
	{
		// Parse as Unix timestamp in MILLISECONDS
		__int64 milliseconds = _tstoi64(trimmed);

		// Convert milliseconds to seconds
		time_t timestamp = (time_t)(milliseconds / 1000);

		return timestamp;
	}

	// Otherwise, try to parse as ISO 8601: YYYY-MM-DDTHH:MM:SS.sssZ
	struct tm timeinfo = { 0 };
	int year, month, day, hour, minute, second;

	int count = _stscanf_s(isoTimestamp, _T("%d-%d-%dT%d:%d:%d"),
		&year, &month, &day, &hour, &minute, &second);

	if (count == 6)
	{
		timeinfo.tm_year = year - 1900;
		timeinfo.tm_mon = month - 1;
		timeinfo.tm_mday = day;
		timeinfo.tm_hour = hour;
		timeinfo.tm_min = minute;
		timeinfo.tm_sec = second;
		timeinfo.tm_isdst = -1;

		// Convert to Unix timestamp
		time_t timestamp = mktime(&timeinfo);

		// Note: This assumes local time. For UTC, we'd need _mkgmtime or adjust for timezone
		// For simplicity, using local time which matches server time in most cases
		return timestamp;
	}

	// If all parsing fails, return current time as fallback
	return time(NULL);
}

// Get or create BarBuilder for a ticker
BarBuilder* GetOrCreateBarBuilder(const CString& ticker)
{
	BarBuilder* pBuilder = NULL;

	EnterCriticalSection(&g_BarBuilderCriticalSection);

	if (!g_BarBuilders.Lookup(ticker, pBuilder))
	{
		// Create new BarBuilder
		pBuilder = new BarBuilder();

		// Extract symbol and exchange from ticker (e.g., "RELIANCE-NSE")
		int dashPos = ticker.ReverseFind('-');
		if (dashPos > 0)
		{
			pBuilder->symbol = ticker.Left(dashPos);
			pBuilder->exchange = ticker.Mid(dashPos + 1);
		}
		else
		{
			pBuilder->symbol = ticker;
			pBuilder->exchange = _T("NSE");  // Default exchange
		}

		g_BarBuilders.SetAt(ticker, pBuilder);
	}

	LeaveCriticalSection(&g_BarBuilderCriticalSection);

	return pBuilder;
}

// Process a tick and update bars
BOOL ProcessTick(const CString& symbol, const CString& exchange, float ltp, float lastTradeQty, time_t timestamp)
{
	static int s_tickCallCount = 0;
	s_tickCallCount++;

	if (!g_bRealTimeCandlesEnabled)
	{
		OutputDebugString(_T("OpenAlgo: ProcessTick - Real-time candles DISABLED, exiting"));
		return FALSE;
	}

	// Create ticker key
	CString ticker = symbol + _T("-") + exchange;

	CString tickLog;
	tickLog.Format(_T("OpenAlgo: ProcessTick #%d START: %s LTP=%.2f Qty=%.0f TS=%lld"),
		s_tickCallCount, ticker, ltp, lastTradeQty, (__int64)timestamp);
	OutputDebugString(tickLog);

	// Get or create BarBuilder
	BarBuilder* pBuilder = GetOrCreateBarBuilder(ticker);
	if (!pBuilder)
	{
		OutputDebugString(_T("OpenAlgo: ProcessTick - FAILED to get/create BarBuilder"));
		return FALSE;
	}

	EnterCriticalSection(&g_BarBuilderCriticalSection);

	// Determine bar boundary (1-minute intervals)
	time_t barPeriodStart = (timestamp / 60) * 60;  // Floor to minute boundary

	CString barLog;
	barLog.Format(_T("OpenAlgo: ProcessTick - BarPeriodStart=%lld CurrentBarStart=%lld NewBar=%d"),
		(__int64)barPeriodStart, (__int64)pBuilder->barStartTime,
		(pBuilder->barStartTime != barPeriodStart) ? 1 : 0);
	OutputDebugString(barLog);

	// Check if we need a new bar
	if (pBuilder->barStartTime != barPeriodStart)
	{
		// Finalize current bar if it exists
		if (pBuilder->bBarStarted && pBuilder->barStartTime > 0)
		{
			CString finalizeLog;
			finalizeLog.Format(_T("OpenAlgo: ProcessTick - Finalizing bar: O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f Ticks=%d"),
				pBuilder->currentBar.Open, pBuilder->currentBar.High, pBuilder->currentBar.Low,
				pBuilder->currentBar.Price, pBuilder->currentBar.Volume, pBuilder->tickCount);
			OutputDebugString(finalizeLog);

			// Add current bar to history
			pBuilder->bars.Add(pBuilder->currentBar);

			// Check if we need to remove old bars (rolling window)
			if (pBuilder->bars.GetCount() >= pBuilder->maxBars)
			{
				// Remove oldest 10% to make room
				int removeCount = pBuilder->maxBars / 10;
				pBuilder->bars.RemoveAt(0, removeCount);
				OutputDebugString(_T("OpenAlgo: ProcessTick - Removed old bars (rolling window)"));
			}
		}

		OutputDebugString(_T("OpenAlgo: ProcessTick - Starting NEW BAR"));

		// Start new bar
		memset(&pBuilder->currentBar, 0, sizeof(struct Quotation));
		pBuilder->currentBar.Open = ltp;
		pBuilder->currentBar.High = ltp;
		pBuilder->currentBar.Low = ltp;
		pBuilder->currentBar.Price = ltp;  // Price is the Close value in Quotation struct
		pBuilder->currentBar.Volume = 0;
		pBuilder->currentBar.OpenInterest = 0;

		// Set normalized timestamp (critical for AmiBroker)
		ConvertUnixToPackedDate(barPeriodStart, &pBuilder->currentBar.DateTime);
		pBuilder->currentBar.DateTime.PackDate.Second = 0;
		pBuilder->currentBar.DateTime.PackDate.MilliSec = 0;
		pBuilder->currentBar.DateTime.PackDate.MicroSec = 0;

		pBuilder->barStartTime = barPeriodStart;
		pBuilder->bBarStarted = TRUE;
		pBuilder->volumeAccumulator = 0.0f;
		pBuilder->bFirstTickReceived = TRUE;
		pBuilder->tickCount = 0;  // Reset tick counter for new bar
	}

	// Update current bar OHLC
	if (ltp > pBuilder->currentBar.High)
		pBuilder->currentBar.High = ltp;
	if (ltp < pBuilder->currentBar.Low || pBuilder->currentBar.Low == 0.0f)
		pBuilder->currentBar.Low = ltp;
	pBuilder->currentBar.Price = ltp;  // Price is the Close value in Quotation struct

	// Accumulate volume
	pBuilder->volumeAccumulator += lastTradeQty;
	pBuilder->currentBar.Volume = pBuilder->volumeAccumulator;
	pBuilder->tickCount++;

	CString updateLog;
	updateLog.Format(_T("OpenAlgo: ProcessTick - Bar UPDATED: O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f Ticks=%d"),
		pBuilder->currentBar.Open, pBuilder->currentBar.High, pBuilder->currentBar.Low,
		pBuilder->currentBar.Price, pBuilder->currentBar.Volume, pBuilder->tickCount);
	OutputDebugString(updateLog);

	// Update last tick time
	pBuilder->lastTickTime = (DWORD)GetTickCount64();

	LeaveCriticalSection(&g_BarBuilderCriticalSection);

	// Notify AmiBroker of update (non-blocking)
	if (g_hAmiBrokerWnd != NULL)
	{
		PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
		OutputDebugString(_T("OpenAlgo: ProcessTick - Sent WM_USER_STREAMING_UPDATE to AmiBroker"));
	}
	else
	{
		OutputDebugString(_T("OpenAlgo: ProcessTick - WARNING: g_hAmiBrokerWnd is NULL, cannot send update"));
	}

	OutputDebugString(_T("OpenAlgo: ProcessTick - SUCCESS, returning TRUE"));
	return TRUE;
}

// Cleanup all BarBuilders
void CleanupBarBuilders(void)
{
	if (g_bBarBuilderCriticalSectionInitialized)
	{
		EnterCriticalSection(&g_BarBuilderCriticalSection);

		// Delete all BarBuilder objects
		POSITION pos = g_BarBuilders.GetStartPosition();
		while (pos != NULL)
		{
			CString ticker;
			BarBuilder* pBuilder;
			g_BarBuilders.GetNextAssoc(pos, ticker, pBuilder);

			if (pBuilder)
			{
				delete pBuilder;
			}
		}

		g_BarBuilders.RemoveAll();

		LeaveCriticalSection(&g_BarBuilderCriticalSection);
	}
}
