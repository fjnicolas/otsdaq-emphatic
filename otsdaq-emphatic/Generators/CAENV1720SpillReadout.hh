//
//  emphatic-artdaq/Generators/CAENV1720SpillReadout.hh
//  Copied from 1730 code - not confirmed to work yet
//

#ifndef emphatic_artdaq_Generators_CAENV1720SpillReadout_hh
#define emphatic_artdaq_Generators_CAENV1720SpillReadout_hh

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/Generators/CommandableFragmentGenerator.hh"
#include "fhiclcpp/fwd.h"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "otsdaq/NetworkUtilities/TCPPublishServer.h"

#include "CAENDigitizer.h"
#include "CAENDigitizerType.h"
#include "otsdaq-emphatic/Overlays/CAENV1720Fragment.hh"

#include "CAENConfiguration.hh"

//#include "CircularBuffer.hh"
#include "PoolBuffer.hh"
#include "workerThread.hh"

#include <mutex>
#include <string>
#include <unordered_map>

namespace emphaticdaq
{
class CAENV1720SpillReadout : public artdaq::CommandableFragmentGenerator
{
  public:
	explicit CAENV1720SpillReadout(fhicl::ParameterSet const& ps);
	virtual ~CAENV1720SpillReadout();

	bool getNext_(artdaq::FragmentPtrs& output) override;
	bool checkHWStatus_() override;
	void start() override;
	void stop() override;
	void stopNoMutex() override { stop(); }
	// void init();

  private:
	bool readSingleWindowFragments(artdaq::FragmentPtrs&);
	bool readSingleWindowDataBlock();
	bool readWindowDataBlocks();

	bool readCombinedWindowFragments(artdaq::FragmentPtrs&);

	void loadConfiguration(fhicl::ParameterSet const& ps);
	void configureInterrupts(unsigned int iboard);

	typedef enum
	{
		CONFIG_READ_ADDR     = 0x8000,
		CONFIG_SET_ADDR      = 0x8004,
		CONFIG_CLEAR_ADDR    = 0x8008,
		TRIGGER_OVERLAP_MASK = 0x0002
	} REGISTERS_t;

	// CAEN pieces
	CAENConfiguration fCAEN;  // initialized in the constructor
	std::vector<int>  fHandle;
	// int                   fHandle;
	CAEN_DGTZ_BoardInfo_t fBoardInfo;
	// char*                 fBuffer;
	uint32_t fBufferSize;
	// uint32_t              fCircularBufferSize;
	CAEN_DGTZ_AcqMode_t fAcqMode;  // initialized in the constructor

	// Send Subrun Messages to SSD
	ots::TCPPublishServer subrun_server_;

	typedef enum
	{
		TEST_PATTERN_S = 3
	} TEST_PATTERN_t;

	typedef enum
	{
		BOARD_READY  = 0x0100,
		PLL_STATUS   = 0x0080,
		PLL_BYPASS   = 0x0040,
		CLOCK_SOURCE = 0x0020,
		EVENT_FULL   = 0x0010,
		EVENT_READY  = 0x0008,
		RUN_ENABLED  = 0x0004
	} ACQ_STATUS_MASK_t;

	typedef enum
	{
		DYNAMIC_RANGE       = 0x8028,
		TRG_OUT_WIDTH       = 0x8070,
		TRG_OUT_WIDTH_CH    = 0x1070,
		ACQ_CONTROL         = 0x8100,
		FP_TRG_OUT_CONTROL  = 0x8110,
		FP_IO_CONTROL       = 0x811C,
		FP_LVDS_CONTROL     = 0x81A0,
		READOUT_CONTROL     = 0xEF00,
		MCST_CONTROL        = 0xEF0C,  // for setting first, intermediate, last board in daisy chain
		DPP_Alo_Feature_Ch1 = 0x1080,
		DPP_Alo_Feature_Ch2 = 0x1180,
		Baseline_Ch1        = 0x1098,
		Baseline_Ch2        = 0x1198,
		Baseline_Ch3        = 0x1298,
		Baseline_Ch4        = 0x1398,
		Baseline_Ch5        = 0x1498,
		Baseline_Ch6        = 0x1598,
		Baseline_Ch7        = 0x1698,
		Baseline_Ch8        = 0x1798,
	} ADDRESS_t;

	typedef enum
	{
		ENABLE_LVDS_TRIGGER  = 0x20000000,
		ENABLE_EXT_TRIGGER   = 0x40000000,
		ENABLE_NEW_LVDS      = 0x100,
		ENABLE_TRG_OUT       = 0xFF,
		TRG_IN_LEVEL         = 0x400,
		TRIGGER_LOGIC        = 0x1F00,
		DISABLE_TRG_OUT_LEMO = 0x2,
		LVDS_IO              = 0x3C,
		LVDS_BUSY            = 0,
		LVDS_TRIGGER         = 1,
		LVDS_nBUSY_nVETO     = 2,
		LVDS_LEGACY          = 3
	} IO_MASK_t;

	enum
	{
		TERROR    = TLVL_ERROR,
		TWARNING  = TLVL_WARNING,
		TINFO     = TLVL_INFO,
		TDEBUG    = TLVL_DEBUG,
		TCONFIG   = 9,
		TSTART    = 10,
		TSTOP     = 11,
		TSTATUS   = 12,
		TGETNEXT  = 13,
		TGETDATA  = 14,
		TMAKEFRAG = 15,
		TTEMP     = 30
	};

	// fhicl parameters
	int      fVerbosity;
	int      fBoardChainNumber;
        uint32_t fNLinks;
        std::vector<uint32_t> fNBoardsPerLink;
	uint32_t fNBoards;
	uint8_t  fInterruptEnable;
	uint32_t fIRQTimeoutMS;
	uint32_t fGetNextSleep;
	uint32_t fGetNextFragmentBunchSize;
	uint32_t fMaxEventsPerTransfer;
	bool     fSWTrigger;
	uint32_t fSelfTriggerMode;
	uint32_t fSelfTriggerMask;
	uint32_t fModeLVDS;
	uint32_t fTrigOutDelay;
	uint32_t fTrigInLevel;
	bool     fCombineReadoutWindows;
	uint32_t fFragmentID;
	bool     fConfigureAfterSpill;

	bool     fUseTimeTagForTimeStamp;
	uint32_t fTimeOffsetNanoSec;
	// Animesh & Aiwu add - dpp algorithm feature 0x1n80
	uint32_t fChargePedstalBitCh1;
	uint32_t fBaselineCh1;
	uint32_t fBaselineCh2;
	uint32_t fBaselineCh3;
	uint32_t fBaselineCh4;
	uint32_t fBaselineCh5;
	uint32_t fBaselineCh6;
	uint32_t fBaselineCh7;
	uint32_t fBaselineCh8;
	// Animesh & Aiwu add end

	// internals
	size_t fNChannels;
	// uint32_t fBoardID; //too hard to manage potential problems if this is a member variable!
	bool     fOK;
	bool     fail_GetNext;
	artdaq::Fragment::sequence_id_t fEvCounter;  // set to zero at the beginning
	uint32_t last_rcvd_rwcounter;
	uint32_t last_sent_rwcounter;
	uint32_t last_sent_ts;
	uint32_t total_data_size;
	// uint32_t event_size;
	uint32_t n_readout_windows;
	uint32_t ch_temps[CAENConfiguration::MAX_CHANNELS];
	uint32_t ch_status[CAENConfiguration::MAX_CHANNELS];

	// functions
	void Configure(uint32_t iboard);

	void                ConfigureDaisyChain(uint32_t iboard);
	void                ConfigureRecordFormat(uint32_t iboard);
	void                ConfigureDataBuffer(uint32_t iboard);
	void                ConfigureTrigger(uint32_t iboard);
	void                ConfigureReadout(uint32_t iboard);
	void                ConfigureAcquisition(uint32_t iboard);
	void                ConfigureSelfTriggerMode(uint32_t iboard);
	CAEN_DGTZ_ErrorCode WriteSPIRegister(int handle, uint32_t ch, uint32_t address, uint8_t value);
	CAEN_DGTZ_ErrorCode ReadSPIRegister(int handle, uint32_t ch, uint32_t address, uint8_t* value);
	void                ReadChannelBusyStatus(int handle, uint32_t ch, uint32_t& status);

	bool readWindowDataBlocksFromBoard(unsigned int iboard);

	bool                        WaitForTrigger(uint32_t iboard);
	bool                        GetData();
	share::WorkerThreadUPtr     GetData_thread_;
	emphaticdaq::PoolBuffer     fPoolBuffer;
	size_t                      fCircularBufferSize;
	std::unique_ptr<uint16_t[]> fBuffer;

	std::unordered_map<uint32_t, artdaq::Fragment::timestamp_t> fTimestampMap;
	mutable std::mutex                                          fTimestampMapMutex;

	// internals in getting the data
	boost::posix_time::ptime         fTimePollEnd, fTimePollBegin;
	boost::posix_time::ptime         fTimeEpoch;
	boost::posix_time::time_duration fTimeDiffPollBegin, fTimeDiffPollEnd;

	artdaq::Fragment::timestamp_t fTS;
	uint64_t                      fMeanPollTime;
	uint64_t                      fMeanPollTimeNS;
	uint32_t                      fTTT;
	long                          fTTT_ns;

	// maps, first index is fragment ID
	std::map<int, bool>                          in_spill_;
	std::map<int, artdaq::Fragment::timestamp_t> spill_start_timestamp_;
	std::map<int, artdaq::FragmentPtrs>          spill_fragments_;
	std::chrono::steady_clock::time_point        last_frag_time_;
	int                                          spill_timeout_ms_;
	size_t                                       subrun_number_{1};
        
        double clock_ns_per_tick_;   //double just in case we use some non-standard clock frequency                     

	void CheckReadback(std::string, int, uint32_t, uint32_t, int channelID = -1);

	CAEN_DGTZ_ErrorCode WriteRegisterBitmask(int32_t handle, uint32_t address, uint32_t data, uint32_t bitmask);
};

}  // namespace emphaticdaq

#endif
