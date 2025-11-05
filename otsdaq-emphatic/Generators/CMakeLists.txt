
include(ArtDictionary)
include(ArtMake)

add_definitions(-DLINUX)

cet_make_library(
  LIBRARY_NAME
    emphatic-artdaq_Utilities
  SOURCE
    workerThread.cpp
    PoolBuffer.cc
  LIBRARIES
    pthread
    messagefacility::MF_MessageLogger
)

list(APPEND LIBRARY_CORE_LIB_LIST emphatic-artdaq_Utilities )

cet_make_library(
  LIBRARY_NAME
    emphatic-artdaq_CAENV1720_GeneratorBase
  SOURCE
    CAENDecoder.cc
    CAENConfiguration.cc
    CAENException.cc
  LIBRARIES
    emphatic-artdaq_Utilities
    fhiclcpp::fhiclcpp
    artdaq::DAQdata # Globals, TRACE
    CAENComm
    CAENVME
    CAENDigitizer
)

include(artdaq::commandableGenerator)


cet_build_plugin(CAENV1720Readout artdaq::commandableGenerator
  LIBRARIES REG
    emphatic-artdaq_CAENV1720_GeneratorBase
    CAENComm
    CAENVME
    CAENDigitizer
  )
  
cet_build_plugin(CAENV1720SpillReadout artdaq::commandableGenerator
  LIBRARIES REG
    emphatic-artdaq_CAENV1720_GeneratorBase
	otsdaq::NetworkUtilities
    CAENComm
    CAENVME
    CAENDigitizer
  )

cet_build_plugin(TRB3Receiver artdaq::commandableGenerator
LIBRARIES REG
  art::Utilities
  otsdaq-emphatic_Overlays
  )

install_source()
install_headers()
