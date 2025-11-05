

#define TRACEMF_USE_VERBATIM 1  // for trace longer path filenames
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME "TRB3Receiver"

#include "otsdaq-emphatic/Generators/TRB3Receiver.hh"

#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Utilities/SimpleLookupPolicy.hh"
#include "artdaq/Generators/GeneratorMacros.hh"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "otsdaq-emphatic/Overlays/FragmentType.hh"
#include "otsdaq-emphatic/Overlays/TRB3Fragment.hh"
#include "otsdaq/Macros/CoutMacros.h"

#include <byteswap.h>
#include <sys/poll.h>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>

//==============================================================================
ots::TRB3Receiver::TRB3Receiver(fhicl::ParameterSet const& ps)
    : CommandableFragmentGenerator(ps)
    , rawOutput_(ps.get<bool>("raw_output_enabled", false))
    , rawPath_(ps.get<std::string>("raw_output_path", "/tmp"))
    , dataport_(ps.get<int>("port", 6343))
    , ip_(ps.get<std::string>("ip", "127.0.0.1"))
    , command_ip_(ps.get<std::string>("command_ip", "192.168.1.153"))
    , commandport_(ps.get<int>("command_port", 2000))
    , rcvbuf_(ps.get<int>("rcvbuf", 0x1000000))
    , spill_timeout_ms_(ps.get<int>("spill_timeout_ms", 5000))
    , spill_start_timestamp_(0)
    , in_spill_(false)
    , expectedPacketNumber_(0)
    , datasocket_(-1)
    , commandsocket_(-1)
    , configure_after_spill_(ps.get<bool>("configure_after_spill", true))
    , receiverThread_(nullptr)
    , fragmentWindow_(ps.get<double>("fragment_time_window_ms", 1000))
    , lastFrag_(std::chrono::steady_clock::now())
{
	TLOG(TLVL_DEBUG) << "Constructor.";

	datasocket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if(!datasocket_)
	{
		throw art::Exception(art::errors::Configuration) << "TRB3Receiver: Error creating socket!";
		exit(1);
	}

	struct sockaddr_in si_me_data;
	si_me_data.sin_family      = AF_INET;
	si_me_data.sin_port        = htons(dataport_);
	si_me_data.sin_addr.s_addr = htonl(INADDR_ANY);
	if(bind(datasocket_, (struct sockaddr*)&si_me_data, sizeof(si_me_data)) == -1)
	{
		throw art::Exception(art::errors::Configuration)
		    << "TRB3Receiver: Cannot bind data socket to port " << dataport_;
		exit(1);
	}
	/*if(fcntl(datasocket_, F_SETFL, O_NONBLOCK) == -1) {

	    throw art::Exception(art::errors::Configuration) << "TRB3Receiver: Cannot set
	  socket to nonblocking!" ;
	  }*/

	if(rcvbuf_ > 0)
	{
		if(setsockopt(datasocket_, SOL_SOCKET, SO_RCVBUF, &rcvbuf_, sizeof(rcvbuf_)))
		{
			throw art::Exception(art::errors::Configuration)
			    << "TRB3Receiver: Could not set receive buffer size: " << rcvbuf_;
			exit(1);
		}

		int       len    = 0;
		socklen_t arglen = sizeof(len);
		int       sts    = getsockopt(datasocket_, SOL_SOCKET, SO_RCVBUF, &len, &arglen);
		if(len < (rcvbuf_ * 2))
		{
			TLOG(TLVL_WARNING) << "RCVBUF " << len << " not expected (" << rcvbuf_ << " sts/errno=" << sts << "/"
			                   << errno;
		}
		else
		{
			TLOG(TLVL_INFO) << "RCVBUF " << len << " sts/errno=" << sts << "/" << errno;
		}
	}

	si_data_.sin_family = AF_INET;
	si_data_.sin_port   = htons(dataport_);
	if(inet_aton(ip_.c_str(), &si_data_.sin_addr) == 0)
	{
		throw art::Exception(art::errors::Configuration)
		    << "TRB3Receiver: Could not translate provided IP Address: " << ip_;
		exit(1);
	}

	if(ps.get<bool>("send_trb_commands", false)) {
	    commandsocket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	    if(!commandsocket_)
	      {
		throw art::Exception(art::errors::Configuration) << "TRB3Receiver: Error creating socket!";
		exit(1);
	      }

	    comm_si_data_.sin_family = AF_INET;
	    comm_si_data_.sin_port   = htons(commandport_);
	    if(inet_aton(command_ip_.c_str(), &comm_si_data_.sin_addr) == 0)
	      {
		throw art::Exception(art::errors::Configuration)
		  << "TRB3Receiver: Could not translate provided IP Address: " << command_ip_;
		exit(1);
	      }
	    int yes = 1;
	    if(setsockopt(commandsocket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	      {
		TLOG(TLVL_ERROR) << "Unable to enable port reuse on message socket, err=" << strerror(errno);
		exit(1);
	      }
	  }
	TLOG(TLVL_INFO) << "TRB3 Receiver Construction Complete!";

	TLOG(TLVL_DEBUG) << "Constructed.";
}  // end constructor()

//==============================================================================
ots::TRB3Receiver::~TRB3Receiver()
{
	TLOG(TLVL_DEBUG) << "Destructor.";

	// join waits for thread to complete
	if(receiverThread_ && receiverThread_->joinable())
	{
		// only join if thread has started
		receiverThread_->join();
		receiverThread_.reset(nullptr);
	}

	if(datasocket_)
	{
		close(datasocket_);
		datasocket_ = -1;
	}

	TLOG(TLVL_DEBUG) << "Destructed.";
}  // end destructor()

//==============================================================================
void ots::TRB3Receiver::start()
{
	TLOG(TLVL_DEBUG) << "Starting...";

	TLOG(TLVL_INFO) << "Starting...";

	subrun_number_ = 1;

	receiverThread_.reset(new std::thread(&TRB3Receiver::receiveLoop_, this));
	sendTRB3Start();

	start_();

	TLOG(TLVL_DEBUG) << "Started.";
}  // end start()

void ots::TRB3Receiver::stop()
{
	TLOG(TLVL_DEBUG) << "Stopping...";

	sendTRB3Stop();
	TLOG(TLVL_DEBUG) << "Stopped.";
}

void ots::TRB3Receiver::sendTRB3Command(std::string command)
{
  if(commandsocket_ != -1) {
	auto sts = sendto(commandsocket_,
	                  command.c_str(),
	                  command.size(),
	                  0,
	                  reinterpret_cast<struct sockaddr*>(&comm_si_data_),
	                  sizeof(comm_si_data_));

	if(sts < 0)
	{
		TLOG(TLVL_WARNING) << "Failed to send " << command << " command to TRB3 Host";
	}
  }
}

//==============================================================================
void ots::TRB3Receiver::receiveLoop_()
{
	while(!should_stop())
	{
		struct pollfd ufds[1];
		ufds[0].fd     = datasocket_;
		ufds[0].events = POLLIN | POLLPRI;

		int rv = poll(ufds, 1, 1000);
		if(rv > 0)
		{
			TLOG(TLVL_TRACE) << "revents: " << ufds[0].revents << ", ";  // ufds[1].revents ;
			if(ufds[0].revents == POLLIN || ufds[0].revents == POLLPRI)
			{
				uint32_t  peekBuffer[10];
				socklen_t dataSz = sizeof(si_data_);
				recvfrom(datasocket_, peekBuffer, sizeof(peekBuffer), MSG_PEEK, (struct sockaddr*)&si_data_, &dataSz);
				auto peekHdr = *reinterpret_cast<TRB3Fragment::TRB3EventHeader*>(&peekBuffer[0]);

				bool need_to_swap = false;
				if(peekHdr.endian_marker_1 != 1 || (peekHdr.endian_marker_2 & 0x80) != 0)
				{
					TLOG(TLVL_TRACE) << "Endian check failed! Marker1 (1): " << peekHdr.endian_marker_1
					                 << ", Marker2 (0): " << static_cast<int>(peekHdr.endian_marker_2 & 0x80);
					need_to_swap = true;
					for(size_t ii = 0; ii < 10; ++ii)
					{
						peekBuffer[ii] = bswap_32(peekBuffer[ii]);
					}
					peekHdr = *reinterpret_cast<TRB3Fragment::TRB3EventHeader*>(&peekBuffer[0]);
					TLOG(TLVL_TRACE) << "After swap: Marker1 (1): " << peekHdr.endian_marker_1
					                 << ", Marker2 (0): " << static_cast<int>(peekHdr.endian_marker_2 & 0x80);
				}

#if 0
				TLOG(TLVL_TRACE) << "peekBuffer:"
				                 << " 0: " << std::hex << static_cast<int>(peekBuffer[0]) << ", 1: " << std::hex
				                 << static_cast<int>(peekBuffer[1]) << ", 2: " << std::hex
				                 << static_cast<int>(peekBuffer[2]) << ", 3: " << std::hex
				                 << static_cast<int>(peekBuffer[3]) << ", 4: " << std::hex
				                 << static_cast<int>(peekBuffer[4]) << ", 5: " << std::hex
				                 << static_cast<int>(peekBuffer[5]) << ", 6: " << std::hex
				                 << static_cast<int>(peekBuffer[6]) << ", 7: " << std::hex
				                 << static_cast<int>(peekBuffer[7]) << ", 8: " << std::hex
				                 << static_cast<int>(peekBuffer[8]) << ", 9: " << std::hex
				                 << static_cast<int>(peekBuffer[9]);
#endif

				packetBuffer_t receiveBuffer;
				auto           word_count =
				    peekHdr.event_size / sizeof(uint32_t) + (peekHdr.event_size % sizeof(uint32_t) == 0 ? 0 : 1);
				receiveBuffer.resize(word_count);
				int sts = recvfrom(
				    datasocket_, &receiveBuffer[0], peekHdr.event_size, 0, (struct sockaddr*)&si_data_, &dataSz);
				// receiveBuffer.resize(sts);

				if(sts == -1)
				{
					TLOG(TLVL_WARNING) << "Error on socket: " << strerror(errno);
				}
				else
				{
					TLOG(TLVL_TRACE) << "Received " << sts << " bytes.";
					if(need_to_swap)
					{
						for(size_t ii = 0; ii < word_count; ++ii)
						{
							receiveBuffer[ii] = bswap_32(receiveBuffer[ii]);
						}
					}
				}

				auto recvHdr = *reinterpret_cast<TRB3Fragment::TRB3EventHeader*>(&receiveBuffer[0]);
				TLOG(TLVL_TRACE) << "DBG HDR  trigger_number: " << static_cast<int>(recvHdr.trigger_number);
				if(expectedPacketNumber_ != recvHdr.trigger_number)
				{
					TLOG(TLVL_WARNING) << "Received trigger_number different than expected! delta="
					                   << static_cast<int>(recvHdr.trigger_number - expectedPacketNumber_)
					                   << " (expected " << static_cast<int>(expectedPacketNumber_) << ", actual "
					                   << static_cast<int>(recvHdr.trigger_number) << ")";
				}
				expectedPacketNumber_ = recvHdr.trigger_number + 1;
#if 0
				auto recvHdr = *reinterpret_cast<TRB3Fragment::TRB3EventHeader*>(&receiveBuffer[0]);
				TLOG(TLVL_TRACE) << "DBG HDR  unknown_word_1: " << static_cast<int>(recvHdr.unknown_word_1);
				TLOG(TLVL_TRACE) << "DBG HDR  unknown_word_2: " << static_cast<int>(recvHdr.unknown_word_2);
				TLOG(TLVL_TRACE) << "DBG HDR  event_size: " << static_cast<int>(recvHdr.event_size);
				TLOG(TLVL_TRACE) << "DBG HDR  endian_marker_1: " << static_cast<int>(recvHdr.endian_marker_1);
				TLOG(TLVL_TRACE) << "DBG HDR  unknown_word_3: " << static_cast<int>(recvHdr.unknown_word_3);
				TLOG(TLVL_TRACE) << "DBG HDR  endian_marker_2: " << static_cast<int>(recvHdr.endian_marker_2);
				TLOG(TLVL_TRACE) << "DBT HDR  central_fpga_id: " << static_cast<int>(recvHdr.central_fpga_id);
				TLOG(TLVL_TRACE) << "DBG HDR  unknown_word_4: " << static_cast<int>(recvHdr.unknown_word_4);
				TLOG(TLVL_TRACE) << "DBG HDR  trigger_number: " << static_cast<int>(recvHdr.trigger_number);

#endif

				std::unique_lock<std::mutex> lock(receiveBufferLock_);
				TLOG(TLVL_TRACE) << "Now placing TRB3 datagram with size " << (int)peekHdr.event_size << " into buffer."
				                 << std::dec;
				receiveBuffers_.push_back(receiveBuffer);
			}
		}
	}
	TLOG(TLVL_INFO) << "receive Loop exiting...";
}

//==============================================================================
bool ots::TRB3Receiver::getNext_(artdaq::FragmentPtrs& output)
{
	if(should_stop())
	{
		return false;
	}

	{
		std::unique_lock<std::mutex> lock(receiveBufferLock_);
		std::move(receiveBuffers_.begin(), receiveBuffers_.end(), std::inserter(packetBuffers_, packetBuffers_.end()));
		receiveBuffers_.clear();
	}

	if(packetBuffers_.size() > 0)
	{
		TLOG(TLVL_TRACE) << "Calling ProcessData, packetBuffers_.size() == " << std::to_string(packetBuffers_.size());
		ProcessData_(spill_fragments_);

		packetBuffers_.clear();
		TLOG(TLVL_TRACE) << "After ProcessData, spill_fragments_.size() " << spill_fragments_.size();
		lastFrag_ = std::chrono::steady_clock::now();
	}
	else
	{
		// Sleep 10 times per poll timeout
		usleep(10000);
		if(in_spill_ && spill_verified_ &&
		   std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - lastFrag_).count() >
		       spill_timeout_ms_)
		{
			in_spill_ = false;

			output.emplace_back(new artdaq::Fragment(subrun_number_, fragment_id()));
			output.back()->setTimestamp(0);  // Container Fragments always have timestamp 0
			artdaq::ContainerFragmentLoader cfl(*output.back());
			cfl.set_missing_data(false);
			cfl.addFragments(spill_fragments_);
			spill_fragments_.clear();
			++subrun_number_;

			if(configure_after_spill_)
			{
				sendTRB3Configure();
			}
			// GetFragmentBuffer()->Reset(false);
			// GetRequestBuffer()->ClearRequests();
		}
	}
	return true;
}

//==============================================================================
void ots::TRB3Receiver::ProcessData_(artdaq::FragmentPtrs& output)
{
	TLOG(TLVL_TRACE) << "ProcessData_ start";

	auto buffer_iter = packetBuffers_.begin();
	while(buffer_iter != packetBuffers_.end())
	{
		auto event_header_ptr = reinterpret_cast<TRB3Fragment::TRB3EventHeader*>(buffer_iter->data());

		TLOG(TLVL_TRACE) << "Creating Fragment";

		artdaq::FragmentPtr thisFrag = artdaq::Fragment::FragmentBytes(event_header_ptr->event_size);

		ev_counter_inc();

		memcpy(thisFrag->dataBegin(), buffer_iter->data(), buffer_iter->size() * sizeof(uint32_t));

		TRB3Fragment ff(*thisFrag);

		// TML: timestamp logic - still needs testing
		// works on data file, but the logic might need changing a bit here
		// This is a little messy because we don't know how big each sub-sub-event is until we get to its header
		// first line is most likely to be wrong - in offline used frag.dataBeginBytes() in place of buffer_iter->data()
		const uint32_t* data_word  = reinterpret_cast<uint32_t const*>(ff.dataBegin() + 1);
		uint32_t        epoch_word = 0;
		uint32_t        tdc_word   = 0;

		// we're now at the start of the first sub sub event. We want stuff from 0502, so skip past the first 2 sses.
		for(unsigned int isse = 0; isse < 2; isse++)
		{
			TRB3Fragment::TRB3SubEventHeader const* sseheader =
			    reinterpret_cast<TRB3Fragment::TRB3SubEventHeader const*>(data_word);
			TLOG(TLVL_TRACE) << "SubEventHeader: "
			                 << "subevent_id: " << std::hex << sseheader->subevent_id
			                 << ", subevent_size: " << sseheader->subevent_size;
			TLOG(TLVL_TRACE) << "Skipping ahead by subevent size " << sseheader->subevent_size
			                 << ", sseheader=" << (void*)sseheader << ", data_word=" << (void*)data_word;
			data_word += sseheader->subevent_size + 1;
		}
		// should now be at start of third sub sub event: 0502
		TRB3Fragment::TRB3SubEventHeader const* sseheader =
		    reinterpret_cast<TRB3Fragment::TRB3SubEventHeader const*>(data_word);
		TLOG(TLVL_TRACE) << "SubEventHeader: "
		                 << "subevent_id: " << std::hex << sseheader->subevent_id
		                 << ", subevent_size: " << sseheader->subevent_size;
		data_word++;
		// loop over words in event and get first epoch and tdc word - should be for channel 0
		for(unsigned int iword = 0; iword < sseheader->subevent_size; iword++)
		{
			if((*(data_word + iword) & 0xe0000000) == 0x60000000)
				epoch_word = *(data_word + iword);
			if((*(data_word + iword) & 0xe0000000) == 0x80000000)
				tdc_word = *(data_word + iword);
			if(epoch_word && tdc_word)
				break;
		}

		if(!(epoch_word && tdc_word))
		{
			TLOG(TLVL_WARNING)
			    << "Did not find an epoch and/or tdc word for 0502! This shouldn't happen. Can't assign a timestamp.";
			TLOG(TLVL_WARNING) << "epoch_word: " << std::hex << epoch_word << " tdc word: " << tdc_word;
			//		  abort();
		}

		double epoch_time  = (double)(epoch_word & 0xfffffff) * 10240.026;  // epoch tick is 10240.026 ns
		double coarse_time = (double)(tdc_word & 0x7ff) * 5;                // coarse tick is 5 ns

		auto timestamp_from_frag = epoch_time + coarse_time;  // in ns

		// auto timestamp_from_frag = event_header_ptr->trigger_number;
		if(!in_spill_)
		{
			spill_start_timestamp_ = timestamp_from_frag;
			in_spill_              = true;
			spill_verified_        = false;
		}

		thisFrag->setSequenceID(event_header_ptr->trigger_number);
		thisFrag->setFragmentID(fragment_id());
		thisFrag->setUserType(ots::detail::FragmentType::TRB3);
		uint64_t fragment_timestamp = timestamp_from_frag - spill_start_timestamp_;
		// fragment_timestamp += (static_cast<uint64_t>(subrun_number_) << 48);
		thisFrag->setTimestamp(fragment_timestamp);
		TLOG(15) << "Generated TRB3 Fragment: "
		         << "Sequence ID: " << thisFrag->sequenceID() << ", Fragment ID: " << thisFrag->fragmentID()
		         << ", ts=" << thisFrag->timestamp();

		std::ofstream rawOutput;

		if(rawOutput_)
		{
			std::string outputPath = rawPath_ + "/TRB3Receiver-" + ip_ + ":" + std::to_string(dataport_) + ".bin";
			rawOutput.open(outputPath, std::ios::out | std::ios::app | std::ios::binary);
			rawOutput.write((const char*)buffer_iter->data(), buffer_iter->size() * sizeof(uint32_t));
		}

		size_t current_event_size = buffer_iter->size() * sizeof(uint32_t);
		while(current_event_size < event_header_ptr->event_size)
		{
			TLOG(11) << "Adding additional UDP datagrams to complete event!";
			++buffer_iter;
			memcpy(thisFrag->dataBeginBytes() + current_event_size,
			       buffer_iter->data(),
			       buffer_iter->size() * sizeof(uint32_t));
			if(rawOutput_)
			{
				rawOutput.write((const char*)buffer_iter->data(), buffer_iter->size() * sizeof(uint32_t));
			}
			current_event_size += buffer_iter->size() * sizeof(uint32_t);
		}

		if(rawOutput_)
		{
			rawOutput.close();
		}

		// First, check if we've met the "spill verification" criteria:
		// 1. This is not the first Fragment in the spill (e.g. positive fragment_timestamp
		// 2. This fragment_timestamp is not over 5s after the last one (if the previous "first in spill" was actually
		// too far in the past)
		if(!spill_verified_ && fragment_timestamp > 0 && fragment_timestamp < 5000000000)
		{
			spill_verified_ = true;
			// We now know that the held_fragment_ was valid, so release it
			if(held_fragment_ != nullptr)
			{
				output.emplace_back(std::move(held_fragment_));
				held_fragment_.reset(nullptr);
			}
			output.emplace_back(std::move(thisFrag));
		}
		// Spill is still not verified, either this is still the first Fragment in the spill, or the previous Fragment
		// was too old
		else if(!spill_verified_)
		{
			// Repalce start timestamp and held_fragment_
			spill_start_timestamp_ = timestamp_from_frag;
			thisFrag->setTimestamp(0);
			held_fragment_.reset(nullptr);
			held_fragment_ = std::move(thisFrag);
		}
		// This is a "verified" spill, so just add the Fragment
		else
		{
			output.emplace_back(std::move(thisFrag));
		}

		++buffer_iter;
	}
	TLOG(TLVL_TRACE) << "ProcessData_ end";
}

//==============================================================================
void ots::TRB3Receiver::stopNoMutex()
{
	//#pragma message "Using default implementation of TRB3Receiver::stopNoMutex()"
}

//==============================================================================
void ots::TRB3Receiver::start_()
{
	//#pragma message "Using default implementation of TRB3Receiver::start_()"
}

// The following macro is defined in artdaq's GeneratorMacros.hh header
DEFINE_ARTDAQ_COMMANDABLE_GENERATOR(ots::TRB3Receiver)
