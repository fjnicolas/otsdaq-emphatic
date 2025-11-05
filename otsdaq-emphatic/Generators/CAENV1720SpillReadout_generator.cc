//
//  emphatic-artdaq/Generators/CAENV1720SpillReadout_generator.cc
//

#define TRACE_NAME "CAENV1720SpillReadout"
#include "artdaq/DAQdata/Globals.hh"

#include "CAENV1720SpillReadout.hh"
#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq/Generators/GeneratorMacros.hh"

#include <time.h>
#include <unistd.h>
#include <iostream>
#include <sstream>

#include <algorithm>
#include "CAENDecoder.hh"
#include "otsdaq-emphatic/Overlays/FragmentType.hh"

#include "boost/date_time/microsec_time_clock.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

using namespace emphaticdaq;

// constructor of the CAENV1720SpillReadout. It wants the param set
// which means the fhicl paramters in CAENV1720SpillReadout.hh

emphaticdaq::CAENV1720SpillReadout::CAENV1720SpillReadout(fhicl::ParameterSet const& ps)
    : CommandableFragmentGenerator(ps), fCAEN(ps), fAcqMode(CAEN_DGTZ_SW_CONTROLLED), subrun_server_(ps.get<int>("subrun_server_port", 8080))
{
	uint32_t data;

	TLOG_ARB(TCONFIG, TRACE_NAME) << "CAENV1720SpillReadout()" << TLOG_ENDL;
//	TLOG(TCONFIG) << fCAEN;
	loadConfiguration(ps);

	last_rcvd_rwcounter = 0x0;
	last_sent_rwcounter = 0x1;
	last_sent_ts        = 0;
	CAEN_DGTZ_ErrorCode retcode;

	fail_GetNext = false;

	fNChannels = fCAEN.nChannels;
	//   fBoardID = fCAEN.boardId; //from old single board code
	uint32_t fBoardID = -1;

	// TML: loop over links and boards per link
	for (unsigned int ilink = 0; ilink < fNLinks; ilink ++)
	{
	  for (unsigned int iboard = 0; iboard < fNBoardsPerLink[ilink]; iboard++)
	    {
	        fBoardID++;
		fHandle.push_back(-1);

		TLOG(TCONFIG) << ": Using BoardID=" << fBoardID << " with NChannels=" << fNChannels;

		TLOG(TDEBUG) << " Calling CAEN_DGTZ_OpenDigitizer(" << CAEN_DGTZ_OpticalLink << ", " << ilink << ", "
		             << iboard << ", " << 0 << " , " << fHandle[fBoardID] << ")";  // AA: debug
		retcode = CAEN_DGTZ_OpenDigitizer(CAEN_DGTZ_OpticalLink, ilink, iboard, 0, &fHandle[fBoardID]);
		TLOG(TDEBUG) << "BoardID: " << fBoardID << " fHandle: " << fHandle[fBoardID];  // TML debug

		fOK = true;

		if(retcode != CAEN_DGTZ_Success)
		{
			emphaticdaq::CAENDecoder::checkError(retcode, "OpenDigitizer", fBoardID);
			CAEN_DGTZ_CloseDigitizer(fHandle[fBoardID]);
			fHandle[fBoardID] = -1;
			fOK               = false;
			TLOG(TLVL_ERROR) << ": Fatal error configuring CAEN board at " << ilink << ", " << iboard;
			TLOG(TLVL_ERROR) << __func__ << ": Terminating process";
			abort();
		}

		retcode = CAEN_DGTZ_Reset(fHandle[fBoardID]);
		emphaticdaq::CAENDecoder::checkError(retcode, "Reset", fBoardID);

		sleep(1);
		Configure(fBoardID);

		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], FP_TRG_OUT_CONTROL, &data);
		TLOG(TLVL_INFO) << "Reg:0x" << std::hex << FP_TRG_OUT_CONTROL << "=0x" << data;

		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], FP_IO_CONTROL, &data);
		TLOG(TLVL_INFO) << "Reg:0x" << std::hex << FP_IO_CONTROL << "=0x" << data;

		// TML: Comment out LVDS parts - not using for EMPHATIC
		// retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID],FP_LVDS_CONTROL,&data);
		// TLOG(TLVL_INFO) << "Reg:0x" << std::hex << FP_LVDS_CONTROL << "=0x" <<
		//   data << std::dec;

		if(!fOK)
		{
			CAEN_DGTZ_CloseDigitizer(fHandle[fBoardID]);
			TLOG(TLVL_ERROR) << ": Fatal error configuring CAEN board at " << ilink << ", " << iboard;
			TLOG(TLVL_ERROR) << __func__ << ": Terminating process";
			abort();
		}
	    } // end loop over board (in link)
	}  // end loop over links

	// Old version without loop:
	// TLOG(TCONFIG) << ": Using BoardID=" << fBoardID << " with NChannels="
	// 		<< fNChannels;

	// TLOG(TDEBUG)  << " Calling CAEN_DGTZ_OpenDigitizer(" <<CAEN_DGTZ_OpticalLink<<",
	// "<<fCAEN.link<<", "<<fBoardChainNumber<<", "<< 0<<" , "<<&fHandle<<")"; //AA: debug
	// retcode = CAEN_DGTZ_OpenDigitizer(CAEN_DGTZ_OpticalLink, fCAEN.link,
	// 				    fBoardChainNumber, 0, &fHandle);

	// fOK=true;

	// if(retcode != CAEN_DGTZ_Success)
	// {
	//   emphaticdaq::CAENDecoder::checkError(retcode,"OpenDigitizer",fBoardID);
	// 		CAEN_DGTZ_CloseDigitizer(fHandle);
	//   fHandle = -1;
	//   fOK = false;
	//   TLOG(TLVL_ERROR) << ": Fatal error configuring CAEN board at " <<
	//     fCAEN.link << ", " << fBoardChainNumber;
	//   TLOG(TLVL_ERROR) << __func__ << ": Terminating process";
	//   abort();
	// }

	// retcode = CAEN_DGTZ_Reset(fHandle);
	// emphaticdaq::CAENDecoder::checkError(retcode,"Reset",fBoardID);

	// sleep(1);
	// Configure();

	// retcode = CAEN_DGTZ_ReadRegister(fHandle,FP_TRG_OUT_CONTROL,&data);
	// TLOG(TLVL_INFO) << "Reg:0x" << std::hex << FP_TRG_OUT_CONTROL <<
	//   "=0x" << data;

	// retcode = CAEN_DGTZ_ReadRegister(fHandle,FP_IO_CONTROL,&data);
	// TLOG(TLVL_INFO) << "Reg:0x" << std::hex << FP_IO_CONTROL << "=0x" << data;

	// // TML: Comment out LVDS parts - not using for EMPHATIC
	// // retcode = CAEN_DGTZ_ReadRegister(fHandle,FP_LVDS_CONTROL,&data);
	// // TLOG(TLVL_INFO) << "Reg:0x" << std::hex << FP_LVDS_CONTROL << "=0x" <<
	// //   data << std::dec;

	// if(!fOK)
	// {
	//   CAEN_DGTZ_CloseDigitizer(fHandle);
	//   TLOG(TLVL_ERROR) << ": Fatal error configuring CAEN board at " <<
	//     fCAEN.link << ", " << fBoardChainNumber;
	//   TLOG(TLVL_ERROR) << __func__ << ": Terminating process";
	//   abort();
	// }

	// Set up worker getdata thread.
	share::ThreadFunctor functor = std::bind(&CAENV1720SpillReadout::GetData, this);
	auto                 worker_functor =
	    share::WorkerThreadFunctorUPtr(new share::WorkerThreadFunctor(functor, "GetDataWorkerThread"));
	auto GetData_worker = share::WorkerThread::createWorkerThread(worker_functor);
	GetData_thread_.swap(GetData_worker);
	TLOG_ARB(TCONFIG, TRACE_NAME) << "GetData worker thread setup." << TLOG_ENDL;

	TLOG(TCONFIG) << "Configuration complete with OK=" << fOK << TLOG_ENDL;

	// epoch time
	fTimeEpoch = boost::posix_time::ptime(boost::gregorian::date(1970, 1, 1));

	// Start Subrun server
	TLOG(TLVL_INFO) << "Starting listen for Subrun server on " << ps.get<int>("subrun_server_port", 8080);
	subrun_server_.startAccept();
}

void emphaticdaq::CAENV1720SpillReadout::configureInterrupts(unsigned int iboard)
{
	uint32_t fBoardID = iboard;

	CAEN_DGTZ_EnaDis_t  state, stateOut;
	uint8_t             interruptLevel, interruptLevelOut;
	uint32_t            statusId, statusIdOut;
	uint16_t            eventNumber, eventNumberOut;
	CAEN_DGTZ_IRQMode_t mode, modeOut;
	CAEN_DGTZ_ErrorCode retcode;

	interruptLevel = 1;  // Fixed for CONET
	statusId       = 1;  // "In the case of the optical link the status_id is meaningless."
	eventNumber    = 1;
	mode           = CAEN_DGTZ_IRQ_MODE_RORA;

	if(fInterruptEnable > 0)  // Enable interrupts
	{
		state = CAEN_DGTZ_ENABLE;
	}
	else  // Disable interrupts
	{
		state = CAEN_DGTZ_DISABLE;
	}

	TLOG(TLVL_INFO) << "Configuring Interrupts state=" << uint32_t{state}
	                << ", interruptLevel=" << uint32_t{interruptLevel} << ", statusId=" << uint32_t{statusId}
	                << ", eventNumber=" << uint32_t{eventNumber} << ", mode=" << int32_t{mode};

	retcode = CAEN_DGTZ_SetInterruptConfig(fHandle[fBoardID], state, interruptLevel, statusId, eventNumber, mode);
	CAENDecoder::checkError(retcode, "SetInterruptConfig", fBoardID);

	retcode = CAEN_DGTZ_GetInterruptConfig(
	    fHandle[fBoardID], &stateOut, &interruptLevelOut, &statusIdOut, &eventNumberOut, &modeOut);
	CAENDecoder::checkError(retcode, "GetInterruptConfig", fBoardID);

	if(state != stateOut)
	{
		TLOG_WARNING("CAENV1720SpillReadout")
		    << "Interrupt State was not setup properly, state write/read=" << int32_t{state} << "/"
		    << int32_t{stateOut};
	}
	if(eventNumber != eventNumberOut)
	{
		TLOG_WARNING("CAENV1720SpillReadout")
		    << "Interrupt State was not setup properly, eventNumber write/read=" << uint32_t{eventNumber} << "/"
		    << uint32_t{eventNumberOut};
	}
	if(statusId != statusIdOut)
	{
		TLOG_WARNING("CAENV1720SpillReadout")
		    << "Interrupt StatusID was not setup properly, eventNumber write/read=" << uint32_t{statusId} << "/"
		    << uint32_t{statusIdOut};
	}
	// Mode and InterruptLevel are only defined on VME, not CONET
	// if (interruptLevel != interruptLevelOut)
	// {
	//   TLOG_WARNING("CAENV1720SpillReadout") << "Interrupt State was not setup properly,
	//   interruptLevel write/read="
	//                                    << uint32_t{interruptLevel}<< "/" <<
	//                                    uint32_t{interruptLevelOut};
	// }
	// if (mode != modeOut)
	// {
	//   TLOG_WARNING("CAENV1720SpillReadout")
	//     << "Interrupt State was not setup properly, mode write/read="
	//     << int32_t { mode }<< "/"<<int32_t { modeOut };
	// }

	uint32_t bitmask = (uint32_t)(0x1FF);
	uint32_t data    = (uint32_t)(0x9);  // RORA,irq link enabled,vme baseaddress relocation
	                                     // disabled,VME Bus error enabled,level 1
	uint32_t addr  = READOUT_CONTROL;
	uint32_t value = 0;
	retcode        = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], addr, &value);
	TLOG(TCONFIG) << "CAEN_DGTZ_ReadRegister prior to overwrite of addr=" << std::hex << addr
	              << ", returned value=" << std::bitset<32>(value);

	TLOG_ARB(TCONFIG, TRACE_NAME) << "Setting I/O control register 0x811C " << TLOG_ENDL;
	retcode = emphaticdaq::CAENV1720SpillReadout::WriteRegisterBitmask(fHandle[fBoardID], addr, data, bitmask);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetIOControl", fBoardID);
	retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], addr, &value);

	TLOG(TCONFIG) << "CAEN_DGTZ_ReadRegister addr=" << std::hex << addr << " and bitmask=" << std::bitset<32>(bitmask)
	              << ", returned value=" << std::bitset<32>(value);
}

void emphaticdaq::CAENV1720SpillReadout::loadConfiguration(fhicl::ParameterSet const& ps)
{
	// initialize the fhicl parameters (see CAENV1720SpillReadout.hh)
	// the obj ps has a member method that gets he private members
	// fVerbosity, etc.. are priv memb in CAENV1720SpillReadout.hh
	//
	// wes, 16Jan2018: disabling default parameters
	///
	fFragmentID = ps.get<std::vector<uint32_t> >("fragment_ids")[0];
	TLOG(TINFO) << __func__ << ": fFragmentID=" << fFragmentID;

	fVerbosity = ps.get<int>("Verbosity");
	TLOG(TINFO) << __func__ << ": Verbosity=" << fVerbosity;

	fBoardChainNumber = ps.get<int>("BoardChainNumber");  // 0
	TLOG(TINFO) << __func__ << ": BoardChainNumber=" << fBoardChainNumber;

	fNLinks = ps.get<uint32_t>("NLinks");
	TLOG(TINFO) << __func__ << ": NLinks=" << fNLinks;

	fNBoardsPerLink = ps.get<std::vector<uint32_t> >("NBoardsPerLink");
	//TLOG(TINFO) << __func__ << ": NBoardsPerLink=" << fNBoardsPerLink;

	fNBoards = ps.get<uint32_t>("NBoards");
	TLOG(TINFO) << __func__ << ": NBoards=" << fNBoards;

	fInterruptEnable = ps.get<uint8_t>("InterruptEnable", 0);
	TLOG(TINFO) << __func__ << ": InterruptEnable=" << (int)fInterruptEnable;

	fIRQTimeoutMS = ps.get<uint32_t>("IRQTimeoutMS", 500);
	TLOG(TINFO) << __func__ << ": IRQTimeoutMS=" << fIRQTimeoutMS;

	fSWTrigger = ps.get<bool>("SWTrigger");  // false
	TLOG(TINFO) << __func__ << ": SWTrigger=" << fSWTrigger;

	fTrigInLevel = ps.get<uint32_t>("TrigInLevel", 0);  // TRG_IN on level (1) or edge (0)
	TLOG(TINFO) << __func__ << ": TrigInLevel=" << fTrigInLevel;

	fSelfTriggerMode = ps.get<uint32_t>("SelfTriggerMode");
	TLOG(TINFO) << __func__ << ": SelfTriggerMode=" << fSelfTriggerMode;

	fSelfTriggerMask = ps.get<uint32_t>("SelfTriggerMask");
	TLOG(TINFO) << __func__ << ": SelfTriggerMask=" << std::hex << fSelfTriggerMask << std::dec;

	fGetNextSleep = ps.get<uint32_t>("GetNextSleep");  // 1000000
	TLOG(TINFO) << __func__ << ": GetNextSleep=" << fGetNextSleep;

	fCircularBufferSize = ps.get<uint32_t>("CircularBufferSize");  // 1000000
	TLOG(TINFO) << __func__ << ": CircularBufferSize=" << fCircularBufferSize;

	fCombineReadoutWindows = ps.get<bool>("CombineReadoutWindows");
	TLOG(TINFO) << __func__ << ": CombineReadoutWindows=" << fCombineReadoutWindows;

	fGetNextFragmentBunchSize = ps.get<uint32_t>("GetNextFragmentBunchSize");
	TLOG(TINFO) << __func__ << ": fGetNextFragmentBunchSize=" << fGetNextFragmentBunchSize;

	fMaxEventsPerTransfer = ps.get<uint32_t>("maxEventsPerTransfer", 1);
	TLOG(TINFO) << __func__ << ": fMaxEventsPerTransfer=" << fMaxEventsPerTransfer;

	fChargePedstalBitCh1 = ps.get<uint32_t>("ChargePedstalBitCh1");  // DPP algorithm feature
	TLOG(TINFO) << __func__ << ": ChargePedstalBitCh1=" << fChargePedstalBitCh1;
	// dc offset or baseline
	fBaselineCh1 = ps.get<uint32_t>("BaselineCh1");  // ch1 baseline
	TLOG(TINFO) << __func__ << ": BaselineCh1=" << fBaselineCh1;
	fBaselineCh2 = ps.get<uint32_t>("BaselineCh2");  // ch2 baseline
	TLOG(TINFO) << __func__ << ": BaselineCh2=" << fBaselineCh2;
	fBaselineCh3 = ps.get<uint32_t>("BaselineCh3");  // ch3 baseline
	TLOG(TINFO) << __func__ << ": BaselineCh3=" << fBaselineCh3;
	fBaselineCh4 = ps.get<uint32_t>("BaselineCh4");  // ch4 baseline
	TLOG(TINFO) << __func__ << ": BaselineCh4=" << fBaselineCh4;
	fBaselineCh5 = ps.get<uint32_t>("BaselineCh5");  // ch5 baseline
	TLOG(TINFO) << __func__ << ": BaselineCh5=" << fBaselineCh5;
	fBaselineCh6 = ps.get<uint32_t>("BaselineCh6");  // ch6 baseline
	TLOG(TINFO) << __func__ << ": BaselineCh6=" << fBaselineCh6;
	fBaselineCh7 = ps.get<uint32_t>("BaselineCh7");  // ch7 baseline
	TLOG(TINFO) << __func__ << ": BaselineCh7=" << fBaselineCh7;
	fBaselineCh8 = ps.get<uint32_t>("BaselineCh8");  // ch8 baseline
	TLOG(TINFO) << __func__ << ": BaselineCh8=" << fBaselineCh8;

	fUseTimeTagForTimeStamp = ps.get<bool>("UseTimeTagForTimeStamp", true);
	TLOG(TINFO) << __func__ << ": fUseTimeTagForTimeStamp=" << fUseTimeTagForTimeStamp;

	fTimeOffsetNanoSec = ps.get<uint32_t>("TimeOffsetNanoSec", 0);  // 0ms by default
	TLOG(TINFO) << __func__ << ": fTimeOffsetNanoSec=" << fTimeOffsetNanoSec;

	spill_timeout_ms_ = ps.get<int>("spill_timeout_ms", 5000);
	TLOG(TINFO) << __func__ << ": spill_timeout_ms_: " << spill_timeout_ms_;

	fConfigureAfterSpill = ps.get<bool>("configure_after_spill", false);
	TLOG(TINFO) << __func__ << ": fConfigureAfterSpill: " << std::boolalpha << fConfigureAfterSpill;

        clock_ns_per_tick_     = ps.get<int>("clock_ns_per_tick", 8);
}

void emphaticdaq::CAENV1720SpillReadout::Configure(unsigned int iboard)
{
	uint32_t fBoardID = iboard;

	TLOG_ARB(TCONFIG, TRACE_NAME) << "Configure()" << TLOG_ENDL;

	CAEN_DGTZ_ErrorCode retcode;
	uint32_t            readback;

	// Make sure DAQ is off first
	TLOG_ARB(TCONFIG, TRACE_NAME) << "Set Acquisition Mode to SW" << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetAcquisitionMode(fHandle[fBoardID], CAEN_DGTZ_SW_CONTROLLED);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetAcquisitionMode", fBoardID);
	retcode = CAEN_DGTZ_GetAcquisitionMode(fHandle[fBoardID], (CAEN_DGTZ_AcqMode_t*)&readback);
	CheckReadback("SetAcquisitionMode", fBoardID, (uint32_t)CAEN_DGTZ_SW_CONTROLLED, readback);

	TLOG_ARB(TCONFIG, TRACE_NAME) << "Stop Acquisition" << TLOG_ENDL;
	retcode = CAEN_DGTZ_SWStopAcquisition(fHandle[fBoardID]);
	emphaticdaq::CAENDecoder::checkError(retcode, "SWStopAcquisition", fBoardID);

	// get info, make sure board is in good communicative state
	retcode = CAEN_DGTZ_GetInfo(fHandle[fBoardID], &fBoardInfo);
	fOK     = (retcode == CAEN_DGTZ_Success);

	retcode = CAEN_DGTZ_Reset(fHandle[fBoardID]);
	sleep(2);

	// ConfigureDaisyChain(); //TML: Maybe this only works for fancier VME crates and
	// isn't actually useful for us at all?
	ConfigureReadout(fBoardID);
	ConfigureRecordFormat(fBoardID);
	ConfigureTrigger(fBoardID);

	if(fAcqMode == CAEN_DGTZ_SW_CONTROLLED)
	{
		TLOG_ARB(TCONFIG, TRACE_NAME) << "Stop Acquisition" << TLOG_ENDL;
		retcode = CAEN_DGTZ_SWStopAcquisition(fHandle[fBoardID]);
		emphaticdaq::CAENDecoder::checkError(retcode, "SWStopAcquisition", fBoardID);
	}

	ConfigureAcquisition(fBoardID);
	configureInterrupts(fBoardID);

	TLOG_ARB(TCONFIG, TRACE_NAME) << "Configure() done." << TLOG_ENDL;
}

void CAENV1720SpillReadout::ReadChannelBusyStatus(int handle, uint32_t ch, uint32_t& status)
{
	status               = 0xdeadbeef;
	uint32_t SPIBusyAddr = 0x1088 + (ch << 8);

	auto ret = CAEN_DGTZ_ReadRegister(handle, SPIBusyAddr, &status);

	if(ret != CAEN_DGTZ_Success)
		TLOG(TLVL_WARNING) << __func__ << ": Failed reading busy status for channel " << ch;
}

// Following SPI code is from CAEN
CAEN_DGTZ_ErrorCode CAENV1720SpillReadout::ReadSPIRegister(int handle, uint32_t ch, uint32_t address, uint8_t* value)
{
	uint32_t            SPIBusy           = 1;
	CAEN_DGTZ_ErrorCode retcod            = CAEN_DGTZ_Success;
	uint32_t            SPIBusyAddr       = 0x1088 + (ch << 8);
	uint32_t            addressingRegAddr = 0x80B4;
	uint32_t            valueRegAddr      = 0x10B8 + (ch << 8);
	uint32_t            val;

	while(SPIBusy)
	{
		if((retcod = CAEN_DGTZ_ReadRegister(handle, SPIBusyAddr, &SPIBusy)) != CAEN_DGTZ_Success)
		{
			return CAEN_DGTZ_CommError;
		}

		SPIBusy = (SPIBusy >> 2) & 0x1;
		if(!SPIBusy)
		{
			if((retcod = CAEN_DGTZ_WriteRegister(handle, addressingRegAddr, address)) != CAEN_DGTZ_Success)
			{
				return CAEN_DGTZ_CommError;
			}

			if((retcod = CAEN_DGTZ_ReadRegister(handle, valueRegAddr, &val)) != CAEN_DGTZ_Success)
			{
				return CAEN_DGTZ_CommError;
			}
		}
		*value = (uint8_t)val;
		usleep(1000);
	}
	return CAEN_DGTZ_Success;
}

CAEN_DGTZ_ErrorCode CAENV1720SpillReadout::WriteSPIRegister(int handle, uint32_t ch, uint32_t address, uint8_t value)
{
	uint32_t            SPIBusy = 1;
	CAEN_DGTZ_ErrorCode retcod  = CAEN_DGTZ_Success;

	uint32_t SPIBusyAddr       = 0x1088 + (ch << 8);
	uint32_t addressingRegAddr = 0x80B4;
	uint32_t valueRegAddr      = 0x10B8 + (ch << 8);

	while(SPIBusy)
	{
		if((retcod = CAEN_DGTZ_ReadRegister(handle, SPIBusyAddr, &SPIBusy)) != CAEN_DGTZ_Success)
		{
			return CAEN_DGTZ_CommError;
		}

		SPIBusy = (SPIBusy >> 2) & 0x1;
		if(!SPIBusy)
		{
			if((retcod = CAEN_DGTZ_WriteRegister(handle, addressingRegAddr, address)) != CAEN_DGTZ_Success)
			{
				return CAEN_DGTZ_CommError;
			}
			if((retcod = CAEN_DGTZ_WriteRegister(handle, valueRegAddr, (uint32_t)value)) != CAEN_DGTZ_Success)
			{
				return CAEN_DGTZ_CommError;
			}
		}
		usleep(1000);
	}
	return CAEN_DGTZ_Success;
}

void emphaticdaq::CAENV1720SpillReadout::ConfigureDaisyChain(uint32_t iboard)
{
	uint32_t            fBoardID = iboard;
	CAEN_DGTZ_ErrorCode retcode;
	uint32_t            readback;
	uint32_t            regValue;

	// Set first board
	if(fBoardID == 0)
	{
		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], MCST_CONTROL, &regValue);
		retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], MCST_CONTROL, (regValue & ~0x300) | 0x200);
		emphaticdaq::CAENDecoder::checkError(retcode, "SetFirstBoard", fBoardID);
		// retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], MCST_CONTROL, 0xaa);
		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], MCST_CONTROL, &readback);
		TLOG(TLVL_INFO) << "Set First Board Reg:0x" << std::hex << MCST_CONTROL << "=0x" << readback;
	}
	// Set last board
	else if(fBoardID == fNBoards - 1)
	{
		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], MCST_CONTROL, &regValue);
		retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], MCST_CONTROL, (regValue & ~0x300) | 0x100);
		emphaticdaq::CAENDecoder::checkError(retcode, "SetLastBoard", fBoardID);
		// retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], MCST_CONTROL, 0xaa);
		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], MCST_CONTROL, &readback);
		TLOG(TLVL_INFO) << "Set Last Board Reg:0x" << std::hex << MCST_CONTROL << "=0x" << readback;
	}
	// Set intermediate boards
	else
	{
		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], MCST_CONTROL, &regValue);
		retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], MCST_CONTROL, (regValue & ~0x300) | 0x300);
		emphaticdaq::CAENDecoder::checkError(retcode, "SetIntBoard", fBoardID);
		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], MCST_CONTROL, &readback);
		TLOG(TLVL_INFO) << "Set Int Board Reg:0x" << std::hex << MCST_CONTROL << "=0x" << readback;
	}
}

void emphaticdaq::CAENV1720SpillReadout::ConfigureSelfTriggerMode(uint32_t iboard)
{
	uint32_t            fBoardID = iboard;
	CAEN_DGTZ_ErrorCode retcod   = CAEN_DGTZ_Success;
	// uint32_t data,readBack;

	retcod =
	    CAEN_DGTZ_SetChannelSelfTrigger(fHandle[fBoardID], (CAEN_DGTZ_TriggerMode_t)fSelfTriggerMode, fSelfTriggerMask);
	emphaticdaq::CAENDecoder::checkError(retcod, "SetSelfTriggerMask", fBoardID);
}

void emphaticdaq::CAENV1720SpillReadout::ConfigureRecordFormat(uint32_t iboard)
{
	uint32_t fBoardID = iboard;
	TLOG_ARB(TCONFIG, TRACE_NAME) << "ConfigureRecordFormat()" << TLOG_ENDL;
	CAEN_DGTZ_ErrorCode retcode;
	uint32_t            readback;

	// channel masks for readout(?)
	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetChannelEnableMask " << fCAEN.channelEnableMask << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetChannelEnableMask(fHandle[fBoardID], fCAEN.channelEnableMask);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetChannelEnableMask", fBoardID);
	retcode = CAEN_DGTZ_GetChannelEnableMask(fHandle[fBoardID], &readback);
	emphaticdaq::CAENDecoder::checkError(retcode, "GetChannelEnableMask", fBoardID);
	CheckReadback("CHANNEL_ENABLE_MASK", fBoardID, fCAEN.channelEnableMask, readback);

	// record length
	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetRecordLength " << fCAEN.recordLength << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetRecordLength(fHandle[fBoardID], fCAEN.recordLength);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetRecordLength", fBoardID);
	retcode = CAEN_DGTZ_GetRecordLength(fHandle[fBoardID], &readback);
	emphaticdaq::CAENDecoder::checkError(retcode, "GetRecordLength", fBoardID);
	CheckReadback("RECORD_LENGTH", fBoardID, fCAEN.recordLength, readback);

	// post trigger size
	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetPostTriggerSize " << (unsigned int)(fCAEN.postPercent) << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetPostTriggerSize(fHandle[fBoardID], (unsigned int)(fCAEN.postPercent));
	emphaticdaq::CAENDecoder::checkError(retcode, "SetPostTriggerSize", fBoardID);
	retcode = CAEN_DGTZ_GetPostTriggerSize(fHandle[fBoardID], &readback);
	emphaticdaq::CAENDecoder::checkError(retcode, "GetPostTriggerSize", fBoardID);
	CheckReadback("POST_TRIGGER_SIZE", fBoardID, fCAEN.postPercent, readback);

	TLOG_ARB(TCONFIG, TRACE_NAME) << "ConfigureRecordFormat() done." << TLOG_ENDL;
}

emphaticdaq::CAENV1720SpillReadout::~CAENV1720SpillReadout()
{
	TLOG_ARB(TCONFIG, TRACE_NAME) << "~CAENV1720SpillReadout()" << TLOG_ENDL;

	if(fBuffer != NULL)
	{
		fBuffer.reset();
	}

	TLOG_ARB(TCONFIG, TRACE_NAME) << "~CAENV1720SpillReadout() done." << TLOG_ENDL;
}

// Taken from wavedump
//  handle : Digitizer handle
//  address: register address
//  data   : value to write to register
//  bitmask: bitmask to override only the bits that need to change while leaving the rest
//  unchanged
CAEN_DGTZ_ErrorCode emphaticdaq::CAENV1720SpillReadout::WriteRegisterBitmask(int32_t  handle,
                                                                             uint32_t address,
                                                                             uint32_t data,
                                                                             uint32_t bitmask)
{
	// int32_t ret = CAEN_DGTZ_Success;
	CAEN_DGTZ_ErrorCode ret = CAEN_DGTZ_Success;
	uint32_t            d32 = 0xFFFFFFFF;
	uint32_t            d32Out;

	ret = CAEN_DGTZ_ReadRegister(handle, address, &d32);
	if(ret != CAEN_DGTZ_Success)
	{
		TLOG(TLVL_ERROR) << __func__ << ": Failed reading a register; address=0x" << std::hex << address;
		abort();
	}

	data &= bitmask;
	d32 &= ~bitmask;
	d32 |= data;
	ret = CAEN_DGTZ_WriteRegister(handle, address, d32);

	if(ret != CAEN_DGTZ_Success)
	{
		TLOG(TLVL_ERROR) << __func__ << ": Failed writing a register; address=0x" << std::hex << address;
		abort();
	}

	ret = CAEN_DGTZ_ReadRegister(handle, address, &d32Out);
	if(ret != CAEN_DGTZ_Success)
	{
		TLOG(TLVL_ERROR) << __func__ << ": Failed reading a register; address=0x" << std::hex << address
		                 << ", value=" << std::bitset<32>(d32) << ", bitmask=" << std::bitset<32>(bitmask);
		abort();
	}

	if(d32 != d32Out)
	{
		TLOG(TLVL_ERROR) << __func__ << ": Read and write values disagree; address=0x" << std::hex << address
		                 << ", read value=" << std::bitset<32>(d32Out) << ", write value=" << std::bitset<32>(d32)
		                 << ", bitmask=" << std::bitset<32>(bitmask);
		abort();
	}

	return ret;
}

void emphaticdaq::CAENV1720SpillReadout::ConfigureDataBuffer(uint32_t iboard)
{
	uint32_t fBoardID = iboard;
	TLOG_ARB(TSTART, TRACE_NAME) << "ConfigureDataBuffer()" << TLOG_ENDL;

	CAEN_DGTZ_ErrorCode retcode;

	//
	retcode = CAEN_DGTZ_SetMaxNumEventsBLT(fHandle[fBoardID], fMaxEventsPerTransfer);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetMaxNumEventsBLT", fBoardID);

	// we do this shenanigans so we can get the BufferSize. We then allocate our own...
	char* myBuffer = NULL;
	retcode        = CAEN_DGTZ_MallocReadoutBuffer(fHandle[fBoardID], &myBuffer, &fBufferSize);
	emphaticdaq::CAENDecoder::checkError(retcode, "MallocReadoutBuffer", fBoardID);

	fBuffer.reset(new uint16_t[fBufferSize / sizeof(uint16_t)]);

	TLOG_ARB(TSTART, TRACE_NAME) << "Created Buffer of size " << fBufferSize << std::endl << TLOG_ENDL;

	retcode = CAEN_DGTZ_FreeReadoutBuffer(&myBuffer);
	emphaticdaq::CAENDecoder::checkError(retcode, "FreeReadoutBuffer", fBoardID);

	if(iboard == 0)
	{
		TLOG_ARB(TSTART, TRACE_NAME) << "Configuring Circular Buffer of size " << fCircularBufferSize << TLOG_ENDL;
		fPoolBuffer.allocate(fBufferSize, fCircularBufferSize, true);
		fPoolBuffer.debugInfo();
	}

	std::lock_guard<std::mutex> lock(fTimestampMapMutex);
	fTimestampMap.clear();
}

void emphaticdaq::CAENV1720SpillReadout::ConfigureTrigger(uint32_t iboard)
{
	uint32_t fBoardID = iboard;
	TLOG_ARB(TCONFIG, TRACE_NAME) << "ConfigureTrigger()" << TLOG_ENDL;

	CAEN_DGTZ_ErrorCode retcode;
	uint32_t            readback;
	uint32_t            addr;

	// set the trigger configurations
	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetSWTriggerMode" << fCAEN.swTrgMode << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetSWTriggerMode(fHandle[fBoardID], (CAEN_DGTZ_TriggerMode_t)(fCAEN.swTrgMode));
	emphaticdaq::CAENDecoder::checkError(retcode, "SetSWTriggerMode", fBoardID);
	retcode = CAEN_DGTZ_GetSWTriggerMode(fHandle[fBoardID], (CAEN_DGTZ_TriggerMode_t*)&readback);
	CheckReadback("SetSWTriggerMode", fBoardID, fCAEN.swTrgMode, readback);

	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetExtTriggerMode" << fCAEN.extTrgMode << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetExtTriggerInputMode(fHandle[fBoardID], (CAEN_DGTZ_TriggerMode_t)(fCAEN.extTrgMode));
	emphaticdaq::CAENDecoder::checkError(retcode, "SetExtTriggerInputMode", fBoardID);
	retcode = CAEN_DGTZ_GetExtTriggerInputMode(fHandle[fBoardID], (CAEN_DGTZ_TriggerMode_t*)&readback);
	CheckReadback("SetExtTriggerInputMode", fBoardID, fCAEN.extTrgMode, readback);

	for(uint32_t ch = 0; ch < fNChannels; ++ch)
	{
		TLOG_ARB(TCONFIG, TRACE_NAME) << "Set channel " << ch << " trigger threshold to " << fCAEN.triggerThresholds[ch]
		                              << TLOG_ENDL;
		retcode = CAEN_DGTZ_SetChannelTriggerThreshold(fHandle[fBoardID], ch, fCAEN.triggerThresholds[ch]);  // 0x8000
		emphaticdaq::CAENDecoder::checkError(retcode, "SetChannelTriggerThreshold", fBoardID);
		retcode = CAEN_DGTZ_GetChannelTriggerThreshold(fHandle[fBoardID], ch, &readback);
		CheckReadback("SetChannelTriggerThreshold", fBoardID, fCAEN.triggerThresholds[ch], readback);

		// pulse width only set in pairs
		/* //Aiwu commented out this because LVDS output width is set elsewhere in the
		ConfigureLVDS() function if(ch%2==0)
		{
		  TLOG_ARB(TCONFIG,TRACE_NAME) << "Set channels " << ch << "/" << ch+1
		               << " trigger pulse width to " << fCAEN.triggerPulseWidth <<
		TLOG_ENDL; retcode =
		CAEN_DGTZ_WriteRegister(fHandle[fBoardID],0x1070+(ch<<8),fCAEN.triggerPulseWidth);
		  emphaticdaq::CAENDecoder::checkError(retcode,"SetChannelTriggerPulseWidth",fBoardID);
		  retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID],0x1070+(ch<<8),&readback);
		  CheckReadback("SetChannelTriggerPulseWidth",fBoardID,fCAEN.triggerPulseWidth,readback);
		}
		*/
	}
	TLOG_ARB(TCONFIG, TRACE_NAME) << "Set global trigger pulse width to " << (int)(fCAEN.triggerPulseWidth)
	                              << TLOG_ENDL;
	retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], TRG_OUT_WIDTH, fCAEN.triggerPulseWidth);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetGlobalTriggerPulseWidth", fBoardID);
	retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], TRG_OUT_WIDTH, &readback);
	CheckReadback("SetGlobalTriggerPulseWidthAll", fBoardID, fCAEN.triggerPulseWidth, readback);
	// Readback must be channel by channel (see reg doc)
	//for(uint32_t ch = 0; ch < CAENConfiguration::MAX_CHANNELS; ch++)
	  //{
	  //uint32_t address = TRG_OUT_WIDTH_CH | (ch << 8);
		//retcode          = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], address, &readback);
		// TLOG(TLVL_DEBUG) << "CAEN_DGTZ_ReadRegister("<<fHandle[fBoardID]<<",
		// "<<address<<", "<<readback<<") returned "<<retcode; //AA: debuggin
		//CheckReadback("SetGlobalTriggerPulseWidth", fBoardID, fCAEN.triggerPulseWidth, readback);
		//TML commenting out for now since we don't really care that this is throwing an error.
		// if(fCAEN.triggerPulseWidth != readback)
		// 	TLOG(TLVL_DEBUG) << "SetGlobalTriggerPulseWidth failed for channel " << ch;
		//}

	ConfigureSelfTriggerMode(fBoardID);

	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetTriggerMode" << fCAEN.extTrgMode << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetExtTriggerInputMode(fHandle[fBoardID], (CAEN_DGTZ_TriggerMode_t)(fCAEN.extTrgMode));
	emphaticdaq::CAENDecoder::checkError(retcode, "SetExtTriggerInputMode", fBoardID);
	retcode = CAEN_DGTZ_GetExtTriggerInputMode(fHandle[fBoardID], (CAEN_DGTZ_TriggerMode_t*)&readback);
	CheckReadback("SetExtTriggerInputMode", fBoardID, fCAEN.extTrgMode, readback);

	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetTriggerOverlap" << fCAEN.allowTriggerOverlap << TLOG_ENDL;
	if(fCAEN.allowTriggerOverlap)
	{
		addr = CONFIG_SET_ADDR;
	}
	else
	{
		addr = CONFIG_CLEAR_ADDR;
	}
	retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], addr, TRIGGER_OVERLAP_MASK);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetTriggerOverlap", fBoardID);

	// level=1 for TTL, =0 for NIM
	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetIOLevel " << (CAEN_DGTZ_IOLevel_t)(fCAEN.ioLevel) << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetIOLevel(fHandle[fBoardID], (CAEN_DGTZ_IOLevel_t)(fCAEN.ioLevel));
	emphaticdaq::CAENDecoder::checkError(retcode, "SetIOLevel", fBoardID);
	retcode = CAEN_DGTZ_GetIOLevel(fHandle[fBoardID], (CAEN_DGTZ_IOLevel_t*)&readback);
	CheckReadback("SetIOLevel", fBoardID, fCAEN.ioLevel, readback);
}

void emphaticdaq::CAENV1720SpillReadout::ConfigureReadout(uint32_t iboard)
{
	uint32_t fBoardID = iboard;
	TLOG_ARB(TCONFIG, TRACE_NAME) << "ConfigureReadout()" << TLOG_ENDL;

	CAEN_DGTZ_ErrorCode retcode;
	uint32_t            readback;
	uint32_t            addr, mask;

	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetRunSyncMode " << (CAEN_DGTZ_RunSyncMode_t)(fCAEN.runSyncMode) << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetRunSynchronizationMode(fHandle[fBoardID], (CAEN_DGTZ_RunSyncMode_t)(fCAEN.runSyncMode));
	emphaticdaq::CAENDecoder::checkError(retcode, "SetRunSynchronizationMode", fBoardID);
	retcode = CAEN_DGTZ_GetRunSynchronizationMode(fHandle[fBoardID], (CAEN_DGTZ_RunSyncMode_t*)&readback);
	CheckReadback("SetRunSynchronizationMode", fBoardID, fCAEN.runSyncMode, readback);

	mask = (1 << TEST_PATTERN_t::TEST_PATTERN_S);
	addr = (fCAEN.testPattern) ? CAEN_DGTZ_BROAD_CH_CONFIGBIT_SET_ADD : CAEN_DGTZ_BROAD_CH_CLEAR_CTRL_ADD;
	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetTestPattern addr=" << addr << ", mask=" << mask << TLOG_ENDL;
	retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], addr, mask);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetTestPattern", fBoardID);

	// Global Registers
	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetDyanmicRange " << fCAEN.dynamicRange << TLOG_ENDL;
	mask    = (uint32_t)(fCAEN.dynamicRange);
	addr    = DYNAMIC_RANGE;
	retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], addr, mask);
	emphaticdaq::CAENDecoder::checkError(retcode, "SetDynamicRange", fBoardID);

	addr = ACQ_CONTROL;
	// AA: I wonder what is the thought behind 0x28? Both 0x20 and 0x8 are "reserved"
	// bits, they do nothing So 0x28 is equivalent to 0. This means:
	// - SW CONTROLLED
	// - Acquisition stop (DISARMED)
	// - Internal oscillator (50 MHz)
	// - LVME I/O Busy disabled
	// - LVME I/O Veto disabled
	// - run on RunIn level
	// - VetoIn not used
	// TML: 0x28 corresponds to bits 3 (trigger counting mode) and 5 (memory full mode).
	retcode = CAEN_DGTZ_WriteRegister(fHandle[fBoardID], addr, uint32_t{0x28});
	emphaticdaq::CAENDecoder::checkError(retcode, "SetTriggerMode", fBoardID);

	uint32_t value = 0;
	retcode        = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], addr, &value);
	emphaticdaq::CAENDecoder::checkError(retcode, "GetTriggerMode", fBoardID);

	TLOG(TCONFIG) << "CAEN_DGTZ_ReadRegister addr=" << std::hex << addr
	              << ", returned value=" << std::bitset<32>(value);

	for(uint32_t ch = 0; ch < fNChannels; ++ch)
	{
		TLOG_ARB(TCONFIG, TRACE_NAME) << "Set channel " << ch << " DC offset to " << fCAEN.pedestal[ch] << TLOG_ENDL;
		retcode = CAEN_DGTZ_SetChannelDCOffset(fHandle[fBoardID], ch, fCAEN.pedestal[ch]);
		emphaticdaq::CAENDecoder::checkError(retcode, "SetChannelDCOffset", fBoardID);
		retcode = CAEN_DGTZ_GetChannelDCOffset(fHandle[fBoardID], ch, &readback);
		CheckReadback("SetChannelDCOffset", fBoardID, fCAEN.pedestal[ch], readback, ch);
	}

	TLOG_ARB(TCONFIG, TRACE_NAME) << "ConfigureReadout() done." << TLOG_ENDL;
}

void emphaticdaq::CAENV1720SpillReadout::ConfigureAcquisition(unsigned int iboard)
{
	uint32_t fBoardID = iboard;
	TLOG_ARB(TCONFIG, TRACE_NAME) << "ConfigureAcquisition() fBoardID=" << fBoardID << TLOG_ENDL;

	CAEN_DGTZ_ErrorCode retcode;
	uint32_t            readback;

	// TML: acquisition mode test
	// if (iboard ==0) {
	TLOG_ARB(TCONFIG, TRACE_NAME) << "SetAcqMode " << (CAEN_DGTZ_AcqMode_t)(fCAEN.acqMode) << TLOG_ENDL;
	retcode = CAEN_DGTZ_SetAcquisitionMode(fHandle[fBoardID], (CAEN_DGTZ_AcqMode_t)(fCAEN.acqMode));
	emphaticdaq::CAENDecoder::checkError(retcode, "SetAcquisitionMode", fBoardID);
	retcode = CAEN_DGTZ_GetAcquisitionMode(fHandle[fBoardID], (CAEN_DGTZ_AcqMode_t*)&readback);
	CheckReadback("SetAcquisitionMode", fBoardID, fCAEN.acqMode, readback);
	//}
	// else {
	//   TLOG_ARB(TCONFIG,TRACE_NAME) << "SetAcqMode " << (CAEN_DGTZ_AcqMode_t)(2) <<
	//   TLOG_ENDL; retcode =
	//   CAEN_DGTZ_SetAcquisitionMode(fHandle[fBoardID],(CAEN_DGTZ_AcqMode_t)(2));
	//   emphaticdaq::CAENDecoder::checkError(retcode,"SetAcquisitionMode",fBoardID);
	//   retcode =
	//   CAEN_DGTZ_GetAcquisitionMode(fHandle[fBoardID],(CAEN_DGTZ_AcqMode_t*)&readback);
	//   CheckReadback("SetAcquisitionMode",fBoardID,2,readback);
	// }
	// TLOG_ARB(TCONFIG,TRACE_NAME) << "SetAnalogMonOutputMode " <<
	// (CAEN_DGTZ_AnalogMonitorOutputMode_t)(fCAEN.analogMode) << TLOG_ENDL; retcode =
	// CAEN_DGTZ_SetAnalogMonOutput(fHandle[fBoardID],(CAEN_DGTZ_AnalogMonitorOutputMode_t)(fCAEN.analogMode));
	// emphaticdaq::CAENDecoder::checkError(retcode,"SetAnalogMonOutputMode",fBoardID);
	// retcode =
	// CAEN_DGTZ_GetAnalogMonOutput(fHandle[fBoardID],(CAEN_DGTZ_AnalogMonitorOutputMode_t*)&readback);
	// CheckReadback("SetAnalogMonOutputMode",fBoardID,fCAEN.analogMode,readback);

	TLOG_ARB(TCONFIG, TRACE_NAME) << "ConfigureAcquisition() done." << TLOG_ENDL;
}

bool emphaticdaq::CAENV1720SpillReadout::WaitForTrigger(uint32_t iboard)
{
	uint32_t fBoardID = iboard;

	TLOG_ARB(TSTATUS, TRACE_NAME) << "WaitForTrigger()" << TLOG_ENDL;

	CAEN_DGTZ_ErrorCode retcode;

	uint32_t acqStatus;
	retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], CAEN_DGTZ_ACQ_STATUS_ADD, &acqStatus);
	if(retcode != CAEN_DGTZ_Success)
	{
		TLOG_WARNING("CAENV1720SpillReadout") << "Trying ReadRegister ACQUISITION_STATUS again." << TLOG_ENDL;
		retcode = CAEN_DGTZ_ReadRegister(fHandle[fBoardID], CAEN_DGTZ_ACQ_STATUS_ADD, &acqStatus);
	}
	emphaticdaq::CAENDecoder::checkError(retcode, "ReadRegister ACQ_STATUS", fBoardID);

	TLOG_ARB(TSTATUS, TRACE_NAME) << " Acq status = " << acqStatus << TLOG_ENDL;
	return (acqStatus & ACQ_STATUS_MASK_t::EVENT_READY);
}

void emphaticdaq::CAENV1720SpillReadout::start()
{
	if(fVerbosity > 0)
		TLOG_INFO("CAENV1720SpillReadout") << "start()" << TLOG_ENDL;
	TLOG_ARB(TSTART, TRACE_NAME) << "start() " << TLOG_ENDL;

	total_data_size     = 0;
	last_sent_rwcounter = 0;
	subrun_number_      = 1;
	// Send the subrun number to all connected clients
	subrun_server_.broadcastPacket(std::to_string(subrun_number_));

	// TML: new loop over all boards
	for(unsigned int iboard = 0; iboard < fNBoards; iboard++)
	{
		uint32_t fBoardID = iboard;

		ConfigureDataBuffer(fBoardID);
		if((CAEN_DGTZ_AcqMode_t)(fCAEN.acqMode) == CAEN_DGTZ_AcqMode_t::CAEN_DGTZ_SW_CONTROLLED)
		{
			CAEN_DGTZ_ErrorCode retcode;
			TLOG_ARB(TSTART, TRACE_NAME) << "SWStartAcquisition fBoardID=" << fBoardID << TLOG_ENDL;
			retcode = CAEN_DGTZ_SWStartAcquisition(fHandle[fBoardID]);
			emphaticdaq::CAENDecoder::checkError(retcode, "SWStartAcquisition", fBoardID);
		}
		else
			TLOG_ARB(TSTART, TRACE_NAME) << "No SWStartAcquisition fBoardID=" << fBoardID << TLOG_ENDL;
	}

	fEvCounter = 0;
	// CAEN_DGTZ_ErrorCode retcod;

	// Animesh add ADC registers here
	// TML: I don't think we need this, unsure though.
	//   for ( uint32_t ch=0; ch<CAENConfiguration::MAX_CHANNELS; ++ch)
	//      {
	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0xFE, 0x00);
	//  // TLOG(TINFO)<<"Write_ADC-CalParams_ch"<<ch<< ": Params[0]=" << CalParams[0];
	//  // emphaticdaq::CAENDecoder::checkError(retcod,"Write_ADC_CalParams_0x20",handle);
	//  // write offset
	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x20, 114);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x21, 107);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x26, 122);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x27, 76);

	// // write gain
	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x22, 14);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x23, 128);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x24, 127);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x28, 14);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x29, 135);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x2A, 125);

	//  // write skew
	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0x70, 129);

	//  // Update parameters
	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0xFE, 0x01);

	//  retcod = WriteSPIRegister(fHandle[fBoardID], ch, 0xFE, 0x00);

	//       }

	//  Animesh ends

	// uint32_t readBack;
	// Animesh start reading the baseline values
	// dc offset or baseline
	// fBaselineCh1 = ps.get<uint32_t>("BaselineCh1"); // ch1 baseline
	TLOG(TINFO) << __func__ << ": Check_BaselineCh1=" << fBaselineCh1;
	//  fBaselineCh2 = ps.get<uint32_t>("BaselineCh2"); // ch2 baseline
	TLOG(TINFO) << __func__ << ": Check_BaselineCh2=" << fBaselineCh2;
	//  fBaselineCh3 = ps.get<uint32_t>("BaselineCh3"); // ch3 baseline
	TLOG(TINFO) << __func__ << ": Check_BaselineCh3=" << fBaselineCh3;
	//  fBaselineCh4 = ps.get<uint32_t>("BaselineCh4"); // ch4 baseline
	TLOG(TINFO) << __func__ << ": Check_BaselineCh4=" << fBaselineCh4;
	//  fBaselineCh5 = ps.get<uint32_t>("BaselineCh5"); // ch5 baseline
	TLOG(TINFO) << __func__ << ": Check_BaselineCh5=" << fBaselineCh5;
	//  fBaselineCh6 = ps.get<uint32_t>("BaselineCh6"); // ch6 baseline
	TLOG(TINFO) << __func__ << ": Check_BaselineCh6=" << fBaselineCh6;
	//  fBaselineCh7 = ps.get<uint32_t>("BaselineCh7"); // ch7 baseline
	TLOG(TINFO) << __func__ << ": Check_BaselineCh7=" << fBaselineCh7;
	//  fBaselineCh8 = ps.get<uint32_t>("BaselineCh8"); // ch8 baseline
	TLOG(TINFO) << __func__ << ": Check_BaselineCh8=" << fBaselineCh8;

	// Animesh ends here

	// Animesh Check trigger threshold here

	for(uint32_t ch = 0; ch < CAENConfiguration::MAX_CHANNELS; ++ch)
	{
		TLOG(TINFO) << "Trigger threshold before run start for ch " << ch << "is " << fCAEN.triggerThresholds[ch];
	}

	// Animesh end

	fTimePollBegin = boost::posix_time::microsec_clock::universal_time();
	GetData_thread_->start();

	// Animesh Check the trigger difference value
	uint32_t Diff_ch1 = fBaselineCh1 - fCAEN.triggerThresholds[0];
	TLOG(TINFO) << "Difference between Baseline and Trig  for Ch 1 is " << Diff_ch1;
	uint32_t Diff_ch2 = fBaselineCh2 - fCAEN.triggerThresholds[1];
	TLOG(TINFO) << "Difference between Baseline and Trig for Ch 2 is " << Diff_ch2;
	uint32_t Diff_ch3 = fBaselineCh3 - fCAEN.triggerThresholds[2];
	TLOG(TINFO) << "Difference between Baseline and Trig for Ch 3 is " << Diff_ch3;
	uint32_t Diff_ch4 = fBaselineCh4 - fCAEN.triggerThresholds[3];
	TLOG(TINFO) << "Difference between Baseline and Trig  for Ch 4 is " << Diff_ch4;
	uint32_t Diff_ch5 = fBaselineCh5 - fCAEN.triggerThresholds[4];
	TLOG(TINFO) << "Difference between Baseline and Trig for Ch 5 is " << Diff_ch5;
	uint32_t Diff_ch6 = fBaselineCh6 - fCAEN.triggerThresholds[5];
	TLOG(TINFO) << "Difference between Baseline and Trig for Ch 6 is " << Diff_ch6;
	uint32_t Diff_ch7 = fBaselineCh7 - fCAEN.triggerThresholds[6];
	TLOG(TINFO) << "Difference between Baseline and Trig for Ch 7 is " << Diff_ch7;
	// Animesh

	TLOG_ARB(TSTART, TRACE_NAME) << "start() done." << TLOG_ENDL;
}

void emphaticdaq::CAENV1720SpillReadout::stop()
{
	if(fVerbosity > 0)
		TLOG_INFO("CAENV1720SpillReadout") << "stop()" << TLOG_ENDL;
	TLOG_ARB(TSTOP, TRACE_NAME) << "stop()" << TLOG_ENDL;

	// Animesh ends

	GetData_thread_->stop();

	CAEN_DGTZ_ErrorCode retcode;
	TLOG_ARB(TSTOP, TRACE_NAME) << "SWStopAcquisition" << TLOG_ENDL;

	for(unsigned int iboard = 0; iboard < fNBoards; iboard++)
	{
		uint32_t fBoardID = iboard;
		retcode           = CAEN_DGTZ_SWStopAcquisition(fHandle[fBoardID]);
		emphaticdaq::CAENDecoder::checkError(retcode, "SWStopAcquisition", fBoardID);
	}

	if(fBuffer != NULL)
	{
		fBuffer.reset();
	}
	TLOG_ARB(TSTOP, TRACE_NAME) << "stop() done." << TLOG_ENDL;
}

bool emphaticdaq::CAENV1720SpillReadout::checkHWStatus_()
{
	for(unsigned int iboard = 0; iboard < fNBoards; iboard++)
	{
		uint32_t fBoardID = iboard;
		for(size_t ch = 0; ch < CAENConfiguration::MAX_CHANNELS; ++ch)
		{
			std::ostringstream tempStream;
			tempStream << "CAENV1720.Card" << fBoardID << ".Channel" << ch << ".Temp";
			std::ostringstream statStream;
			statStream << "CAENV1720.Card" << fBoardID << ".Channel" << ch << ".Status";
			std::ostringstream memfullStream;
			memfullStream << "CAENV1720.Card" << fBoardID << ".Channel" << ch << ".MemoryFull";

			CAEN_DGTZ_ReadTemperature(fHandle[fBoardID], ch, &(ch_temps[ch]));
			TLOG_ARB(TTEMP, TRACE_NAME) << tempStream.str() << ": " << ch_temps[ch] << "  C" << TLOG_ENDL;

			metricMan->sendMetric(tempStream.str(), int(ch_temps[ch]), "C", 1, artdaq::MetricMode::Average);

			ReadChannelBusyStatus(fHandle[fBoardID], ch, ch_status[ch]);
			TLOG_ARB(TTEMP, TRACE_NAME) << statStream.str() << std::hex << ": 0x" << ch_status[ch] << std::dec
			                            << TLOG_ENDL;

			if(ch_status[ch] == 0xdeadbeef)
			{
				TLOG(TLVL_WARNING) << __func__ << ": Failed reading busy status for channel " << ch;
			}
			else
			{
				metricMan->sendMetric(statStream.str(), int(ch_status[ch]), "", 1, artdaq::MetricMode::LastPoint);

				metricMan->sendMetric(
				    memfullStream.str(), int((ch_status[ch] & 0x1)), "", 1, artdaq::MetricMode::LastPoint);

				/*
				metricMan->sendMetric("MemoryEmpty", int((ch_status[ch] & 0x2)>>1), "", 1,
				            artdaq::MetricMode::LastPoint,tempStream.str());

				metricMan->sendMetric("DACBusy", int((ch_status[ch] & 0x4)>>2), "", 1,
				            artdaq::MetricMode::LastPoint,tempStream.str());

				metricMan->sendMetric("ADCPowerDown", int((ch_status[ch] & 0x100)>>8), "",
				1, artdaq::MetricMode::LastPoint,tempStream.str());
				*/
			}
		}
	}

	return true;
}

bool emphaticdaq::CAENV1720SpillReadout::GetData()
{
	TLOG(TGETDATA) << __func__ << ": Begin of GetData()";

	CAEN_DGTZ_ErrorCode retcod;

	// TML: This should always be false for EMPHATIC, right?
	// AA: yes, I believe so (unless we find use of saving triggers generated by software
	// - perhaps for synchronization?)
	if(fSWTrigger)
	{
		usleep(fGetNextSleep);
		TLOG(TGETDATA) << "Sending SW trigger..." << TLOG_ENDL;
		for(unsigned int iboard = 0; iboard < fNBoards; iboard++)
		{
			uint32_t fBoardID = iboard;
			retcod            = CAEN_DGTZ_SendSWtrigger(fHandle[fBoardID]);
			TLOG(TGETDATA) << "CAEN_DGTZ_SendSWtrigger returned " << int{retcod};
		}
	}

	// read the data from the buffer of the card
	// this_data_size is the size of the acq window

	// return readSingleWindowDataBlock();

	return readWindowDataBlocks();

}  // CAENV1720SpillReadout::GetData()

bool emphaticdaq::CAENV1720SpillReadout::readWindowDataBlocksFromBoard(unsigned int iboard)
{
	uint32_t fBoardID = iboard;

	TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__ << ": Begin. fBoardID=" << fBoardID
	               << " fHandle[fBoardID]=" << fHandle[fBoardID];

	auto t_IRQWait_start = boost::posix_time::microsec_clock::universal_time();  // AA: added for debugging
	// wait for one event, then interrupt
	// AA: why do we call this function if we disabled interrupt mode in the fhicl file?!
//	CAEN_DGTZ_ErrorCode retcode = CAEN_DGTZ_IRQWait(fHandle[fBoardID], fIRQTimeoutMS); //AA, Jan 31, commented
	// CAEN_DGTZ_ErrorCode retcode = CAEN_DGTZ_IRQWait(fHandle[0], fIRQTimeoutMS);
	CAEN_DGTZ_ErrorCode retcode = CAEN_DGTZ_Success; //AA, Jan 31, uncommented
	auto t_IRQWait_end = boost::posix_time::microsec_clock::universal_time();
	TLOG(TGETDATA) << "(iboard=" << iboard << ")"
	               << "CAEN_DGTZ_IRQWait took " << (t_IRQWait_end - t_IRQWait_start).total_milliseconds() << " ms ";

	// if we have a timeout condition, return
	if(retcode == CAEN_DGTZ_Timeout)
	{
		// end of this poll
		fTimePollEnd = boost::posix_time::microsec_clock::universal_time();

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
		               << ": Exiting after a timeout. Poll time was "
		               << (fTimePollEnd - fTimePollBegin).total_milliseconds() << " ms.";

		// update the polling time for the next poll
		fTimePollBegin = fTimePollEnd;

		// go again!
		return true;
	}

	TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
	               << ": No timeout. TimePollBegin=" << fTimePollBegin << " TimePollEnd=" << fTimePollEnd;

	uint32_t read_data_size = 1;
	size_t   n_reads        = 0;

	// gianluca won't let me do a do while
	// we want to do ReadData until there is no more data to read

	TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
	               << "Start while loop read. " << read_data_size;
	while(read_data_size != 0)
	{
		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
		               << "Last read data size was " << read_data_size;

		// reset read_data_size to 0, just in case
		read_data_size = 0;

		// get a block of data from the PoolBuffer. Hopefully doesn't take very long.

		auto block = fPoolBuffer.takeFreeBlock();
		if(!block)
		{
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
			                 << ": PoolBuffer is empty; last received trigger sequenceID=" << last_rcvd_rwcounter;
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
			                 << ": PoolBuffer status: freeBlockCount=" << fPoolBuffer.freeBlockCount()
			                 << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")"
			                 << ", activeBlockCount=" << fPoolBuffer.activeBlockCount();
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
			                 << ": Critical error; aborting boardreader process....";

			fail_GetNext = true;

			std::this_thread::yield();
			return false;
		}
		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
		               << ": Got a free DataBlock from PoolBuffer";

		// call ReadData
		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
		               << ": Calling ReadData(fHandle[fBoardID]=" << fHandle[fBoardID]
		               << ",bufp=" << (void*)block->begin << ",&block.size=" << (void*)&(block->size) << ")";

		retcode = CAEN_DGTZ_ReadData(
		    fHandle[fBoardID], CAEN_DGTZ_SLAVE_TERMINATED_READOUT_MBLT, (char*)block->begin, &read_data_size);

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
		               << "This read data size was " << read_data_size;
		if(read_data_size == 0)
		{
			fPoolBuffer.returnFreeBlock(block);
			break;
		}

		++n_reads;

		block->verify_redzone();
		block->data_size = read_data_size;

		// check to make sure no errors on readout.
		if(retcode != CAEN_DGTZ_Success)
		{
			TLOG(TLVL_ERROR) << __func__
			                 << ": CAEN_DGTZ_ReadData returned non zero return code; return code=" << int{retcode};
			fPoolBuffer.returnFreeBlock(block);
			std::this_thread::yield();
			return false;
		}

		fTimePollEnd = boost::posix_time::microsec_clock::universal_time();
		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
		               << ": CAEN_DGTZ_ReadData complete with returned data size " << block->data_size
		               << " retcod=" << int{retcode};

		// const auto header = reinterpret_cast<CAENV1720EventHeader const
		// *>(block->begin);
		auto header = reinterpret_cast<CAENV1720EventHeader*>(
		    block->begin);  // AA hack: make it non-const in order to modify boardID

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
		               << ": 1720_EVENT_COUNTER=" << header->eventCounter << ", 1720_EVENT_SIZE=" << header->eventSize
		               << ", 1720_TIME_TAG=" << header->triggerTimeTag << ", 1720_BOARD_ID=" << header->boardID;

		header->boardID = fBoardID;  // AA hack, boardID in the header is meaningless (always 0), so we
		                             // overwrite it with board number. Needed to set fragment ID later!
		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")"
		               << " Set board_ID to: " << header->boardID;

		const size_t header_event_size = sizeof(uint32_t) * header->eventSize;
		if(block->data_size < header_event_size)
		{
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
			                 << ": Wrong event size; returned=" << block->data_size << ", header=" << header_event_size
			                 << ". 1720_EVENT_COUNTER=" << header->eventCounter
			                 << ", 1720_EVENT_SIZE=" << header->eventSize
			                 << ", 1720_TIME_TAG=" << header->triggerTimeTag << ". DROPPING THIS FRAGMENT.";
			fPoolBuffer.returnFreeBlock(block);
			break;
		}

		// TML: Don't need this for EMPHATIC?
		// //do all the timestamp assignment
		// //first reference against epoch
		// fTimeDiffPollBegin = fTimePollBegin - fTimeEpoch;
		// fTimeDiffPollEnd = fTimePollEnd - fTimeEpoch;

		// //then calculate the mean poll time
		// fMeanPollTime = fTimeDiffPollBegin.total_nanoseconds()/2 +
		// fTimeDiffPollEnd.total_nanoseconds()/2; fMeanPollTimeNS =
		// fMeanPollTime%(1000000000); fTTT=0; fTTT_ns = -1;

		// if(fUseTimeTagForTimeStamp){
		//   fTTT = uint32_t{header->triggerTimeTag}; //
		//   fTTT_ns = fTTT*8;

		//   // Scheme borrowed from what Antoni developed for CRT.
		//   // See
		//   https://sbn-docdb.fnal.gov/cgi-bin/private/DisplayMeeting?sessionid=7783 fTS
		//   = fMeanPollTime - fMeanPollTimeNS + fTTT_ns
		// 	+ (fTTT_ns - (long)fMeanPollTimeNS < -500000000) * 1000000000
		// 	- (fTTT_ns - (long)fMeanPollTimeNS >  500000000) * 1000000000
		// 	- fTimeOffsetNanoSec;
		// }
		// else{
		//   fTS = fTimeDiffPollEnd.total_nanoseconds() - fTimeOffsetNanoSec;;
		// }

		// //put lock in local scope
		// {
		//   std::lock_guard<std::mutex> lock(fTimestampMapMutex);
		//   fTimestampMap[uint32_t{header->eventCounter}] = fTS;
		// }

		// //print out timestamping info
		// TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")"
		// 		   << "TIMESTAMP " << fFragmentID
		// 		   << ": Poll begin/end/mean/ns = " <<
		// fTimeDiffPollBegin.total_nanoseconds()
		// 		   << "/" << fTimeDiffPollEnd.total_nanoseconds()
		// 		   << "/" << fMeanPollTime
		// 		   << "/" << fMeanPollTimeNS;
		// TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")"
		// 		   << "TIMESTAMP " << fFragmentID
		// 		   << ": TTT/TTT_ns/TS_ns = " << fTTT << "/" << fTTT_ns << "/" << fTS;
		// TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")"
		// 		   << "TIMESTAMP " << fFragmentID
		// 		   << ": Timestamp for event " << header->eventCounter << " = " << fTS;

		// check trigger counter gaps
		auto readoutwindow_trigger_counter_gap = uint32_t{header->eventCounter} - last_rcvd_rwcounter;
		if(readoutwindow_trigger_counter_gap > 1u)
		{
			TLOG(TLVL_DEBUG) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
			                 << " : Missing triggers; previous trigger sequenceID / gap  = " << last_rcvd_rwcounter
			                 << " / " << readoutwindow_trigger_counter_gap
			                 << ", freeBlockCount=" << fPoolBuffer.freeBlockCount()
			                 << ", activeBlockCount=" << fPoolBuffer.activeBlockCount()
			                 << ", fullyDrainedCount=" << fPoolBuffer.fullyDrainedCount();
		}
		last_rcvd_rwcounter = uint32_t{header->eventCounter};

		// return active block
		fPoolBuffer.returnActiveBlock(block);

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
		               << ": CAEN_DGTZ_ReadData returned DataBlock header.eventCounter=" << header->eventCounter
		               << ", header.eventSize=" << header_event_size;
		//} // TML: End loop over boards here?

	}  // end while read_data_size is not zero

	TLOG(TGETDATA) << "(FragID=" << fFragmentID << " fBoardID=" << fBoardID << ")" << __func__
	               << ": n_reads=" << n_reads << " fBoardID=" << fBoardID;

	return true;
}  // readWindowDataBlocksFromBoard

bool emphaticdaq::CAENV1720SpillReadout::readWindowDataBlocks()
{
	if(fail_GetNext)
	{
		TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << ")" << __func__
		                 << " : Not calling CAEN_DGTZ_ReadData due a previous critical error...";
		::usleep(50000);
		return false;
	}

	// TML: new loop over all boards
	bool retVal = true;
	for(unsigned int iboard = 0; iboard < fNBoards; iboard++)
		if(!readWindowDataBlocksFromBoard(iboard))
			retVal = false;

	// update the polling time for the next poll
	fTimePollBegin = fTimePollEnd;

	TLOG(TGETDATA) << "(FragID=" << fFragmentID << ") fNBoards=" << fNBoards << " retVal=" << retVal;
	// and go again!
	return retVal;
}

// old single block code ... probably should not be used anymore
bool emphaticdaq::CAENV1720SpillReadout::readSingleWindowDataBlock()
{
	// RAR : commented out for emphatic test beam
	// AA: we are not using this function in EMPHATIC 2022, right?
	throw std::runtime_error("Critical error; do not call readSingleWindowDataBlock....");
	uint32_t fBoardID = 0;
	if(fail_GetNext)
	{
		TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << ")" << __func__
		                 << " : Not calling CAEN_DGTZ_ReadData due a previous critical error...";
		::usleep(50000);
		return false;
	}

	TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__ << ": Begin of readSingleWindowDataBlock()";

	CAEN_DGTZ_ErrorCode retcode = CAEN_DGTZ_IRQWait(fHandle[fBoardID], fIRQTimeoutMS);

	fTimePollEnd = boost::posix_time::microsec_clock::universal_time();

	fTimeDiffPollBegin = fTimePollBegin - fTimeEpoch;
	fTimeDiffPollEnd   = fTimePollEnd - fTimeEpoch;

	fTimePollBegin = boost::posix_time::microsec_clock::universal_time();

	if(retcode == CAEN_DGTZ_Timeout)
	{
		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__ << ": Exiting after a timeout. Poll time was "
		               << (fTimeDiffPollEnd - fTimeDiffPollBegin).total_milliseconds() << " ms.";
		return true;
	}
	else if(retcode != CAEN_DGTZ_Success)
	{
		TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << ")" << __func__
		                 << ": CAEN_DGTZ_IRQWait returned non zero return code; return code=" << int{retcode};
		std::this_thread::yield();
		return false;
	}

	auto fragment_count = fGetNextFragmentBunchSize;

	metricMan->sendMetric(
	    "Free DataBlocks", fPoolBuffer.freeBlockCount(), "fragments", 1, artdaq::MetricMode::LastPoint);
	while(--fragment_count)
	{
		auto block = fPoolBuffer.takeFreeBlock();

		if(!block)
		{
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << ")" << __func__
			                 << ": PoolBuffer is empty; last received trigger sequenceID=" << last_rcvd_rwcounter;
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << ")" << __func__
			                 << ": PoolBuffer status: freeBlockCount=" << fPoolBuffer.freeBlockCount()
			                 << "(FragID=" << fFragmentID << ")"
			                 << ", activeBlockCount=" << fPoolBuffer.activeBlockCount();
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << ")" << __func__
			                 << ": Critical error; aborting boardreader process....";

			fail_GetNext = true;

			std::this_thread::yield();
			return false;
		}

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__ << ": Got a free DataBlock from PoolBuffer";

		uint32_t read_data_size = 0;

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__
		               << ": Calling ReadData(fHandle[fBoardID]=" << fHandle[fBoardID]
		               << ",bufp=" << (void*)block->begin << ",&block.size=" << (void*)&(block->size) << ")";

		retcode = CAEN_DGTZ_ReadData(
		    fHandle[fBoardID], CAEN_DGTZ_SLAVE_TERMINATED_READOUT_MBLT, (char*)block->begin, &read_data_size);

		block->verify_redzone();
		block->data_size = read_data_size;

		if(retcode != CAEN_DGTZ_Success)
		{
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << ")" << __func__
			                 << ": CAEN_DGTZ_ReadData returned non zero return code; return code=" << int{retcode};
			fPoolBuffer.returnFreeBlock(block);
			std::this_thread::yield();
			return false;
		}

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__
		               << ": CAEN_DGTZ_ReadData complete with returned data size " << block->data_size
		               << " retcod=" << int{retcode};

		if(block->data_size == 0)
		{
			TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__
			               << ": CAEN_DGTZ_ReadData returned zero data size";
			fPoolBuffer.returnFreeBlock(block);
			std::this_thread::yield();
			return false;
		}

		const auto   header            = reinterpret_cast<CAENV1720EventHeader const*>(block->begin);
		const size_t header_event_size = sizeof(uint32_t) * header->eventSize;

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__ << ": PMT_EVENT_COUNTER=" << header->eventCounter
		               << ", PMT_EVENT_SIZE=" << header->eventSize << ", PMT_TIME_TAG=" << header->triggerTimeTag;

		if(block->data_size < header_event_size)
		{
			TLOG(TLVL_ERROR) << "(FragID=" << fFragmentID << ")" << __func__
			                 << ": Wrong event size; returned=" << block->data_size << ", header=" << header_event_size;
		}
		/*
		fMeanPollTime = fTimeDiffPollBegin.total_nanoseconds()/2 +
		fTimeDiffPollEnd.total_nanoseconds()/2; fMeanPollTimeNS =
		fMeanPollTime%(1000000000); fTTT=0; fTTT_ns = -1;

		if(fUseTimeTagForTimeStamp){
		  fTTT = uint32_t{header->triggerTimeTag}; //
		  fTTT_ns = fTTT*8;


		  // Scheme borrowed from what Antoni developed for CRT.
		  // See https://sbn-docdb.fnal.gov/cgi-bin/private/DisplayMeeting?sessionid=7783
		  fTS = fMeanPollTime - fMeanPollTimeNS + fTTT_ns
		+ (fTTT_ns - (long)fMeanPollTimeNS < -500000000) * 1000000000
		- (fTTT_ns - (long)fMeanPollTimeNS >  500000000) * 1000000000
		- fTimeOffsetNanoSec;

		}
		else{
		  fTS = fTimeDiffPollEnd.total_nanoseconds() - fTimeOffsetNanoSec;;
		}

		{
		  std::lock_guard<std::mutex> lock(fTimestampMapMutex);
		  fTimestampMap[uint32_t{header->eventCounter}] = fTS;
		}

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")"
		       << __func__
		       << "TIMESTAMP " <<  fFragmentID
		       << ": Poll begin/end/mean/ns = " << fTimeDiffPollBegin.total_nanoseconds()
		       << "/" << fTimeDiffPollEnd.total_nanoseconds()
		       << "/" << fMeanPollTime
		       << "/" << fMeanPollTimeNS;
		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")"
		       << "TIMESTAMP " << fFragmentID
		       << ": TTT/TTT_ns/TS_ns = " << fTTT << "/" << fTTT_ns << "/" << fTS;
		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")"
		       << "TIMESTAMP " << fFragmentID
		       << ": Timestamp for event " << header->eventCounter << " = " << fTS;



		auto readoutwindow_trigger_counter_gap= uint32_t{header->eventCounter} -
		last_rcvd_rwcounter;

		if( readoutwindow_trigger_counter_gap > 1u ){
		  TLOG (TLVL_DEBUG) << "(FragID=" << fFragmentID << ")"
		        << __func__ << " : Missing triggers; previous trigger sequenceID / gap  =
		" << last_rcvd_rwcounter << " / "
		        << readoutwindow_trigger_counter_gap <<", freeBlockCount="
		<<fPoolBuffer.freeBlockCount()
		        << ", activeBlockCount=" <<fPoolBuffer.activeBlockCount() << ",
		fullyDrainedCount=" << fPoolBuffer.fullyDrainedCount();
		}
		*/
		last_rcvd_rwcounter = uint32_t{header->eventCounter};
		fPoolBuffer.returnActiveBlock(block);

		TLOG(TGETDATA) << "(FragID=" << fFragmentID << ")" << __func__
		               << ": CAEN_DGTZ_ReadData returned DataBlock header.eventCounter=" << header->eventCounter
		               << ", header.eventSize=" << header_event_size;
	}

	return true;
}

// this is really the DAQ part where the server reads data from
// the card and stores them
bool emphaticdaq::CAENV1720SpillReadout::getNext_(artdaq::FragmentPtrs& fragments)
{
	TLOG(TGETNEXT) << __func__ << ": Begin of getNext_()";
	if(fail_GetNext)
		throw std::runtime_error("Critical error; stopping boardreader process....");
	return fCombineReadoutWindows ? readCombinedWindowFragments(fragments) : readSingleWindowFragments(fragments);
}

bool emphaticdaq::CAENV1720SpillReadout::readSingleWindowFragments(artdaq::FragmentPtrs& fragments)
{
	TLOG(TGETNEXT) << __func__ << ": Begin of readSingleWindowFragments()";

	static auto start = std::chrono::steady_clock::now();

	std::chrono::duration<double> delta = std::chrono::steady_clock::now() - start;

	if(delta.count() > 0.005 * fGetNextFragmentBunchSize)
	{
		metricMan->sendMetric("Laggy getNext", 1, "count", 1, artdaq::MetricMode::Accumulate);
		TLOG(TLVL_DEBUG) << __func__ << ": Time spent outside of getNext_() " << delta.count() * 1000
		                 << " ms. Last seen fragment sequenceID=" << last_sent_rwcounter;
	}

	if(fPoolBuffer.activeBlockCount() == 0)
	{
		TLOG(TGETNEXT) << __func__
		               << ": PoolBuffer has no data.  Laast last seen fragment sequenceID=" << last_sent_rwcounter
		               << "; Sleep for " << fGetNextSleep << " us and return.";
		::usleep(fGetNextSleep);
		start = std::chrono::steady_clock::now();

		// AA: use only first fragmentID to determine whether to start new spill.
		// an alternative would be to use any fragmentID but that could fail in
		// case the very first spill misses some fragments.
		// This method would fail only if first fragmentID board was completely dead
		if(in_spill_[fFragmentID] == true &&
		   std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_frag_time_)
		           .count() > spill_timeout_ms_)
		{
			for(auto& s : in_spill_)
			{
				s.second = false;
			}

			if(fConfigureAfterSpill)
			{
				for(unsigned int iboard = 0; iboard < fNBoards; iboard++)
					Configure(iboard);
			}

			for(auto& frags : spill_fragments_)
			{
				fragments.emplace_back(new artdaq::Fragment(subrun_number_, frags.first));
				fragments.back()->setTimestamp(0);  // Container Fragments always have timestamp 0
				artdaq::ContainerFragmentLoader cfl(*fragments.back());
				cfl.set_missing_data(false);
				cfl.addFragments(frags.second);
			}
			spill_fragments_.clear();

			artdaq::FragmentPtr endOfSubrunFrag(new artdaq::Fragment(static_cast<size_t>(
			    ceil(sizeof(my_rank) / static_cast<double>(sizeof(artdaq::Fragment::value_type))))));

			endOfSubrunFrag->setSystemType(artdaq::Fragment::EndOfSubrunFragmentType);

			endOfSubrunFrag->setSequenceID(subrun_number_ + 1);
			endOfSubrunFrag->setTimestamp(subrun_number_ + 1);

			*endOfSubrunFrag->dataBegin() = my_rank;
			++subrun_number_;
			// Send the subrun number to all connected clients
			subrun_server_.broadcastPacket(std::to_string(subrun_number_));
			fragments.emplace_back(std::move(endOfSubrunFrag));
		}

		return true;
	}

	double          max_fragment_create_time = 0.0;
	double          min_fragment_create_time = 10000.0;
	struct timespec now;
	clock_gettime(CLOCK_REALTIME, &now);
	const auto metadata = CAENV1720FragmentMetadata(
	    0 /*fBoardID not needed*/, fNChannels, fCAEN.recordLength, now.tv_sec, now.tv_nsec, ch_temps);
	const auto fragment_datasize_bytes = metadata.ExpectedDataSize();
	TLOG(TMAKEFRAG) << __func__ << ": Created CAENV1720FragmentMetadata with expected data size of "
	                << fragment_datasize_bytes << " bytes.";

	// auto fragment_count=fGetNextFragmentBunchSize;

	// just get anything that's there...
	while(fPoolBuffer.activeBlockCount())
	{
		start = std::chrono::steady_clock::now();

		auto pool_block = fPoolBuffer.takeActiveBlock();
		if(pool_block == nullptr)
		{
			TLOG(TLVL_ERROR) << "Logic error ?! activeBlockCount() is non zero, but takeActiveBlock failed!";
			continue;
		}

		auto                            iter    = pool_block->begin;
		auto                            end     = pool_block->begin + pool_block->data_size;
		bool                            first   = true;
		artdaq::Fragment::fragment_id_t frag_id = 0;

		while(iter < end)
		{
			TLOG(21) << __func__ << ": b4 FragmentBytes";
			auto fragment_uptr = artdaq::Fragment::FragmentBytes(
			    fragment_datasize_bytes, fEvCounter, fFragmentID, ots::detail::FragmentType::CAENV1720, metadata);

			TLOG(21) << __func__ << ": b4 memcpy poolBuffer->Fragment";
			memcpy(fragment_uptr->dataBegin(), iter, fragment_uptr->dataSizeBytes());
			iter += fragment_uptr->dataSizeBytes();

			auto header = reinterpret_cast<CAENV1720EventHeader*>(fragment_uptr->dataBeginBytes());
			if(first)
			{
				frag_id = header->boardID;
				first   = false;
			}
			header->boardID                        = frag_id;
			const auto readoutwindow_event_counter = uint32_t{header->eventCounter};
			fragment_uptr->setSequenceID(readoutwindow_event_counter);
			TLOG(TLVL_TRACE) << "boardid=" << header->boardID
			                 << " readoutwindow_event_counter = " << readoutwindow_event_counter;

			// TML: setting fragment ID with header board ID
			const int& thisFragmentID = frag_id;
			fragment_uptr->setFragmentID(frag_id);

			// AA: compute timestamp for EMPHATIC 2022
			// timestamp is defined as time in ns since the first event of the spill

			// artdaq::Fragment::timestamp_t
			uint64_t ts_frag = header->triggerTime();
			if(!in_spill_[thisFragmentID])
			{
				in_spill_[thisFragmentID]              = true;
				spill_start_timestamp_[thisFragmentID] = ts_frag;
			}
			const uint64_t rollover_shift =
			    (ts_frag >= spill_start_timestamp_[thisFragmentID]) ? 0 : 0x7fffffff;  // 2³¹–1
			uint64_t fragment_timestamp = (ts_frag + rollover_shift - spill_start_timestamp_[thisFragmentID]) * clock_ns_per_tick_;
			// fragment_timestamp += (static_cast<uint64_t>(subrun_number_) << 48);
			fragment_uptr->setTimestamp(fragment_timestamp);

			// auto readoutwindow_event_counter_gap= readoutwindow_event_counter -
			// last_sent_rwcounter;

			TLOG(TMAKEFRAG) << __func__ << ": Created fragment " << fFragmentID << " for event "
			                << readoutwindow_event_counter << " triggerTimeTag " << header->triggerTimeTag
			                << " ts=" << fragment_timestamp;

			/*
			if( readoutwindow_event_counter_gap > 1u ){
			  if ( last_sent_rwcounter > 0 )
			  {
			TLOG (TLVL_DEBUG) << __func__ << ": Missing data; previous fragment sequenceID /
			gap  = " << last_sent_rwcounter << " / "
			                    << readoutwindow_event_counter_gap;
			metricMan->sendMetric("Missing Fragments",
			uint64_t{readoutwindow_event_counter_gap}, "frags", 1,
			artdaq::MetricMode::Accumulate);
			  }
			}

			if( readoutwindow_event_counter < last_sent_rwcounter )
			  {
			TLOG(TLVL_ERROR) << __func__ << " SequnceIDs processed out of order!! "
			         << readoutwindow_event_counter << " < " << last_sent_rwcounter <<
			TLOG_ENDL;
			  }
			if( last_sent_ts > ts_frag)
			  {
			TLOG(TLVL_ERROR) << __func__ << " Timestamps out of order!! Last event later than
			current one."
			         << ts_frag << " < " << last_sent_ts << TLOG_ENDL;
			  }
			*/
			spill_fragments_[thisFragmentID].emplace_back(nullptr);
			std::swap(spill_fragments_[thisFragmentID].back(), fragment_uptr);

			if(thisFragmentID == 0)
			{
				fEvCounter++;
			}
			last_sent_rwcounter = readoutwindow_event_counter;
			last_sent_ts        = ts_frag;
		}

		pool_block->clear_data();
		fPoolBuffer.returnFreeBlock(pool_block);

		delta           = std::chrono::steady_clock::now() - start;
		last_frag_time_ = std::chrono::steady_clock::now();

		min_fragment_create_time = std::min(delta.count(), min_fragment_create_time);
		max_fragment_create_time = std::max(delta.count(), max_fragment_create_time);

		if(delta.count() > 0.0005)
		{
			metricMan->sendMetric("Laggy Fragments", 1, "frags", 1, artdaq::MetricMode::Maximum);
			TLOG(TLVL_DEBUG) << __func__ << ": Creating a fragment with setSequenceID=" << last_sent_rwcounter
			                 << " took " << delta.count() * 1000 << " ms";
			// TRACE_CNTL("modeM", 0);
		}
	}

	metricMan->sendMetric(
	    "Fragment Create Time  Max", max_fragment_create_time, "s", 1, artdaq::MetricMode::Accumulate);
	// metricMan->sendMetric("Fragment Create Time  Min"
	// ,min_fragment_create_time,"s",1,artdaq::MetricMode::Accumulate);

	// wes ... this shouldn't be called here!
	// checkHWStatus_();

	TLOG(TGETNEXT) << __func__ << ": End of readSingleWindowFragments(); returning " << fragments.size()
	               << " fragments.";

	start = std::chrono::steady_clock::now();

	return true;
}

bool emphaticdaq::CAENV1720SpillReadout::readCombinedWindowFragments(artdaq::FragmentPtrs&)
{
	throw std::runtime_error("Implement me correctly!");
}

void emphaticdaq::CAENV1720SpillReadout::CheckReadback(
    std::string label, int boardID, uint32_t wrote, uint32_t readback, int channelID)
{
	if(wrote != readback)
	{
		std::stringstream channelLabel(" ");
		if(channelID >= 0)
			channelLabel << " Ch/Grp " << channelID;

		std::stringstream text;
		text << " " << label << " ReadBack error BoardId " << boardID << channelLabel.str() << " wrote " << wrote
		     << " read " << readback;
		TLOG(TLVL_ERROR) << __func__ << ": " << text.str();

		// emphaticdaq::CAENException e(CAEN_DGTZ_DigitizerNotReady,
		//			     text.str(), boardId);
		// throw(e);
	}
	else
	{  // AA: debug information
		std::stringstream channelLabel(" ");
		if(channelID >= 0)
			channelLabel << " Ch/Grp " << channelID;

		std::stringstream text;
		text << " " << label << " ReadBack correct BoardId " << boardID << channelLabel.str() << " wrote " << wrote
		     << " read " << readback;

		TLOG(TLVL_DEBUG) << __func__ << ": " << text.str();
	}
}

DEFINE_ARTDAQ_COMMANDABLE_GENERATOR(emphaticdaq::CAENV1720SpillReadout)
