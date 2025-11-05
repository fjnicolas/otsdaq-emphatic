include(BuildPlugins) # Art Module

cet_build_plugin(WFViewer art::module LIBRARIES REG otsdaq-acc::Overlays ROOT::Core ROOT::Gui otsdaq::MessageFacility)


cet_build_plugin(CAENV1720Dump art::module
	LIBRARIES REG
		art_root_io::TFileService_service
    messagefacility::MF_MessageLogger
    ROOT::Hist
    ROOT::Core
    ROOT::Geom
    ROOT::Tree
    otsdaq-acc::Overlays
    artdaq-core::Data
    art_root_io::TFileService_service
)

install_source()
install_fhicl()
install_headers()
