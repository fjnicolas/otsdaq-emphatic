#cet_make_library(LIBRARY_NAME Overlays
#   SOURCE FragmentType.cc 
#   LIBRARIES PUBLIC
#   artdaq-core::Data
# )

#cet_make_library(LIBRARY_NAME otsdaq-emphatic_Overlays
#SOURCE
#CAENV1720Fragment.cc
#FragmentType.cc
#TRB3Fragment.cc
#LIBRARIES PUBLIC
#artdaq-core_Data
#)

#basic_plugin(OtsDemo "fragmentNameHelper"
#  otsdaq-demo_Overlays
#  #artdaq_core::artdaq-core_Data_Artdaq_fragmentNameHelper
#  artdaq-core_Data_Artdaq_fragmentNameHelper
#)

art_make(LIBRARY_NAME otsdaq-emphatic_Overlays
    EXCLUDE OtsDemo_fragmentNameHelper.cc
       LIB_LIBRARIES
       artdaq-core_Data
       cetlib_except::cetlib_except
       )

#basic_plugin(OtsDemo "fragmentNameHelper"
#  otsdaq-emphatic_Overlays
#  #artdaq-core_Data_Artdaq_fragmentNameHelper
#  artdaq-core_fragmentNameHelper
#)

include(ArtdaqCorePlugins)
cet_build_plugin(OtsDemo artdaq::fragmentNameHelper LIBRARIES REG otsdaq-emphatic::Overlays)
#include(ArtdaqCorePlugins)
#cet_build_plugin(OtsDemo artdaq::fragmentNameHelper LIBRARIES REG Overlays)

install_headers()
install_source()
