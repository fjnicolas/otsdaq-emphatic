//-------------------------------------------------
//---------------------------------------

////////////////////////////////////////////////////////////////////////
// Class:       CAENV1720Dump
// Module Type: analyzer
// File:        CAENV1720Dump_module.cc
// Description: Prints out information about each event.
////////////////////////////////////////////////////////////////////////

#include "art/Framework/Core/EDAnalyzer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"

#include "canvas/Utilities/Exception.h"

#include "otsdaq-emphatic/Overlays/CAENV1720Fragment.hh"
#include "otsdaq-emphatic/Overlays/FragmentType.hh"
#include "artdaq-core/Data/Fragment.hh"

#include "art_root_io/TFileService.h"
#include "TH1F.h"
#include "TH1S.h"
#include "TNtuple.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <vector>
#include <iostream>
#include <bitset>

namespace emphaticdaq {
  class CAENV1720Dump;
}

/**************************************************************************************************/

class emphaticdaq::CAENV1720Dump : public art::EDAnalyzer {

public:
  struct Config {
    //--one atom for each parameter
    fhicl::Atom<art::InputTag> DataLabel {
      fhicl::Name("data_label"),
      fhicl::Comment("Tag for the input data product")
    };
    fhicl::Atom<int> Shift {
      fhicl::Name("shift_fragment_id"), 
      fhicl::Comment("Number to subtract to the fragment_id")
    };
    fhicl::Atom<int> EventToPlot {
      fhicl::Name("event_to_plot"), 
      fhicl::Comment("Event number to be shown in histograms")
    };
    fhicl::Atom<int> NumberOfBoards {
      fhicl::Name("number_of_boards"), 
      fhicl::Comment("Number of V1720 boards ")
    };
    fhicl::Atom<int> NumberOfChannels {
      fhicl::Name("number_of_channels"), 
      fhicl::Comment("Number of V1720 channels")
    };
    fhicl::Atom<int> WaveformLength {
      fhicl::Name("waveform_length"), 
      fhicl::Comment("Number of samples in each waveform")
    };
  }; //--configuration
  using Parameters = art::EDAnalyzer::Table<Config>;

  explicit CAENV1720Dump(Parameters const & pset);
  virtual ~CAENV1720Dump();

  void analyze(const art::Event& evt) override;
  void beginJob() override;
  void endJob() override;

private:

  uint16_t nChannels;
  static const uint16_t MAX_MAX_N_CHANNELS = 128;
  uint16_t MaxnChannels;
  uint32_t waveform_length;
  uint16_t number_of_boards;
  uint32_t Ttt_DownSamp;
 /* the trigger time resolution is 16ns when waveforms are sampled at
                               * 500MHz sampling. The trigger timestamp is thus
                               * sampled 4 times slower than input channels*/

  TNtuple* nt_header;
  
  TH1F*    hEventCounter;
  TH1F*    hTriggerTimeTag;
  TH1S*    h_wvfm_ev0[MAX_MAX_N_CHANNELS];

  TTree* fEventTree;
  uint16_t fRun;
  uint16_t fSubrun;
  uint16_t fFragmentID;
  uint64_t fTimestamp;
  art::EventNumber_t fEvent;
  std::vector<uint64_t>  fTicksVec;
  std::vector< std::vector<uint16_t> >  fWvfmsVec;
  
  unsigned int EventToPlot;
  art::InputTag fDataLabel;
	
  int fShift; 

}; //--class CAENV1720Dump


emphaticdaq::CAENV1720Dump::CAENV1720Dump(CAENV1720Dump::Parameters const& pset): art::EDAnalyzer(pset)
{
  fDataLabel  = pset().DataLabel();
  fShift      = pset().Shift();
  EventToPlot = pset().EventToPlot();
  MaxnChannels= pset().NumberOfChannels();
  if(MaxnChannels > MAX_MAX_N_CHANNELS) {
    TLOG(TLVL_ERROR)<<"MaxnChannels = "<<(int)MaxnChannels<<" exceeds maximum allowed size of "<<MAX_MAX_N_CHANNELS;
  }
  waveform_length = pset().WaveformLength();
  number_of_boards = pset().NumberOfBoards();
}

void emphaticdaq::CAENV1720Dump::beginJob()
{
  art::ServiceHandle<art::TFileService> tfs; 
  nt_header       = tfs->make<TNtuple>("nt_header","CAENV1720 Header Ntuple","art_ev:caen_ev:caenv_ev_tts"); 
  
  hEventCounter   = tfs->make<TH1F>("hEventCounter","Event Counter Histogram",10000,0,10000);
  hTriggerTimeTag = tfs->make<TH1F>("hTriggerTimeTag","Trigger Time Tag Histogram",10,2000000000,4500000000);
  for(int board = 0; board < number_of_boards; board++)
    for(int ch = 0; ch < MaxnChannels; ch++)
      h_wvfm_ev0[ch + board*MaxnChannels]  = tfs->make<TH1S>(TString::Format("h_wvfm_ev%d_board%d_ch%d", EventToPlot, board, ch),TString::Format("Waveform for event %d, board %d, channel %d", EventToPlot, board, ch),waveform_length,0,waveform_length);
  
  //--make tree to store the channel waveform info:
  fEventTree = tfs->make<TTree>("events","waveform tree");
  fEventTree->Branch("fRun",&fRun,"fRun/s");
  fEventTree->Branch("fSubrun",&fSubrun,"fSubrun/s");
  fEventTree->Branch("fEvent",&fEvent,"fEvent/I");
  fEventTree->Branch("fTicksVec",&fTicksVec);
  fEventTree->Branch("fWvfmsVec",&fWvfmsVec);
  fEventTree->Branch("fTimestamp",&fTimestamp, "fTimestamp/l");
  fEventTree->Branch("fFragmentID",&fFragmentID, "fFragmentID/s");
}

void emphaticdaq::CAENV1720Dump::endJob() {
  std::cout << "Ending CAENV1720Dump...\n";
}

emphaticdaq::CAENV1720Dump::~CAENV1720Dump() { }

void emphaticdaq::CAENV1720Dump::analyze(const art::Event& evt)
{
  fRun    = evt.run();
  fSubrun = evt.subRun();
  fEvent  = evt.event();

    
  std::vector<art::Handle<artdaq::Fragments>>fragmentHandles = evt.getMany<artdaq::Fragments>();
  TLOG(TLVL_INFO)<<"Run "<<evt.run() << ", subrun " << evt.subRun() << ", event " << fEvent;
  for (const auto & handle : fragmentHandles) {
    if (!handle.isValid() || handle->size() == 0) 
      continue;

    if (handle->front().type() == artdaq::Fragment::ContainerFragmentType) {
      TLOG(TLVL_WARNING)<<"Container type, can't handle this handle!";
    }
    else if(handle->front().type() == ots::detail::FragmentType::CAENV1720) {
      TLOG(TLVL_INFO)<<"This is CAENV1720 fragment";
      fWvfmsVec.resize(16*handle->size());

      TLOG(TLVL_INFO)<<"Looping over "<<handle->size()<<" fragments";

      for(const auto& frag : *handle) { //loop over fragments
        //--use this fragment as a reference to the same data
        CAENV1720Fragment bb(frag);
        auto const* md = bb.Metadata();
        CAENV1720Event const* event_ptr = bb.Event();

        CAENV1720EventHeader header = event_ptr->Header;

        fFragmentID = frag.fragmentID();

        //convert fragment access time to human-readable format
        struct timespec ts = frag.atime();
        char buff[100];
        strftime(buff, sizeof buff, "%D %T", gmtime(&ts.tv_sec));

        fTimestamp = frag.timestamp();

        TLOG(TLVL_INFO) 
          << "\n\tFrom fragment header, typeString "  << frag.typeString()
          << "\n\tFrom fragment header, timestamp  "  << (frag.timestamp()/1'000'000'000) <<" s, "<< std::setfill('0')<<std::setw(9)<<(frag.timestamp()%1'000'000'000)<<std::setw(0)<<" ns"
          << "\n\tFrom fragment header, sequenceID "  << frag.sequenceID()
          << "\n\tFrom fragment header, fragmentID "  << fFragmentID
          << "\n\tFrom fragment header, size       "  << frag.size()
          << "\n\tFrom fragment header, atime      "  << buff<<" "<<ts.tv_sec<<" ns" 
          << "\n\tFrom header, eventSize           "  << header.eventSize
          << "\n\tFrom header, marker              "  << header.marker
          << "\n\tFrom header, channelMask         "  << header.channelMask
          << "\n\tFrom header, pattern             "  << header.pattern
          << "\n\tFrom header, eventFormat         "  << header.eventFormat
          << "\n\tFrom header, reserved            "  << header.reserved 
          << "\n\tFrom header, boardFail           "  << header.boardFail
          << "\n\tFrom header, boardID             "  << header.boardID
          << "\n\tFrom header, event counter       "  << header.eventCounter
          << "\n\tFrom header, triggerTimeTag      "  << header.triggerTimeTag
          << "\n\tFrom header, triggerTimeRollover "  << header.triggerTimeRollOver()
          << "\n\tFrom header, extendedTriggerTime "  << header.extendedTriggerTime()
          << "\n\tFrom metadata, boardID           "  << md->boardID
          << "\n\tFrom metadata, nChannels         "  << md->nChannels
          << "\n\tFrom metadata, nSamples          "  << md->nSamples
          << "\n\tFrom metadata, timeStampSec      "  << md->timeStampSec
          << "\n\tFrom metadata, timeStampNSec     "  << md->timeStampNSec
          << "\n\tFrom metadata, chTemps           "  
             << md->chTemps[0]<<", "
             << md->chTemps[1]<<", "
             << md->chTemps[2]<<", "
             << md->chTemps[3]<<", "
             << md->chTemps[4]<<", "
             << md->chTemps[5]<<", "
             << md->chTemps[6]<<", "
             << md->chTemps[7];
        fFragmentID -= fShift;
        TLOG(TLVL_INFO)
          << "\n\tShifted fragment id is "  << fFragmentID;

        uint32_t t0 = header.triggerTimeTag;
        hEventCounter->Fill(header.eventCounter);
        hTriggerTimeTag->Fill(t0);
        nt_header->Fill(fEvent,header.eventCounter,t0);
        nChannels = md->nChannels;
        std::cout << "\tNumber of channels: " << nChannels << "\n";

        const int board = header.boardID;

        //--get the number of 32-bit words (quad_bytes) from the header
        uint32_t ev_size_quad_bytes = header.eventSize;
        uint32_t evt_header_size_quad_bytes = sizeof(CAENV1720EventHeader)/sizeof(uint32_t);
        uint32_t data_size_double_bytes = 2*(ev_size_quad_bytes - evt_header_size_quad_bytes);
        std::cout << "Data size = " << data_size_double_bytes << "\n";
        uint32_t wfm_length = data_size_double_bytes/nChannels;
        std::cout << "Channel waveform length = " << wfm_length << "\n";

        //--store the tick value for each acquisition 
        fTicksVec.resize(wfm_length);

        const uint16_t* data_begin = reinterpret_cast<const uint16_t*>(frag.dataBeginBytes() + sizeof(CAENV1720EventHeader));

        const uint16_t* value_ptr =  data_begin;
        uint16_t value = 0;
        size_t ch_offset = 0; 
        //--loop over channels
        std::cout<<"Looping over "<<nChannels<<" channels\n";
        for (size_t i_ch=0; i_ch<nChannels; ++i_ch){
          if (i_ch >= MaxnChannels) { 
            TLOG(TLVL_ERROR)<<"found channel "<<i_ch<<" larger than "<<(MaxnChannels)<<"! How could it happen? Debug, debug!";
            break;
          }
          fWvfmsVec[i_ch+nChannels*fFragmentID].resize(wfm_length);
          ch_offset = (size_t)(i_ch * wfm_length);

          //--loop over waveform samples
          for(size_t i_t=0; i_t<wfm_length; ++i_t){ 
            fTicksVec[i_t] = t0*Ttt_DownSamp + i_t;   //timestamps, event level
            value_ptr = data_begin + ch_offset + i_t; //pointer arithmetic
            value = *(value_ptr);
	    if(fEvent == EventToPlot) {
	      h_wvfm_ev0[i_ch + board*MaxnChannels]->SetBinContent(i_t+1,value);
	    }
            fWvfmsVec[i_ch+nChannels*fFragmentID][i_t] = value;
          }  //--end loop samples
        } //--end loop channels
        fEventTree->Fill();
      } //loop over fragments
    } //valid handle
    else {
      TLOG(TLVL_INFO)<<"This is not CAENV1720 fragment";
    }
  } //loop over handles
} //analyze

DEFINE_ART_MODULE(emphaticdaq::CAENV1720Dump)

