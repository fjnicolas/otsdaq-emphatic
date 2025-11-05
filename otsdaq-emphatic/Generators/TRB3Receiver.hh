#ifndef artdaq_ots_Generators_TRB3Receiver_hh
#define artdaq_ots_Generators_TRB3Receiver_hh

// The TRB3 Receiver class receives TRB3 data from an otsdaq application and
// puts that data into TRB3Fragments for further ARTDAQ analysis.
//
// It currently assumes two things to be true:
// 1. The first word of the TRB3 packet is an 8-bit flag with information
// about the status of the sender
// 2. The second word is an 8-bit sequence ID, used for detecting
// dropped TRB3 datagrams

// Some C++ conventions used:

// -Append a "_" to every private member function and variable

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/Generators/CommandableFragmentGenerator.hh"
#include "fhiclcpp/fwd.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <list>
#include <mutex>
#include <queue>
#include <thread>

namespace ots
{
typedef std::vector<uint32_t>     packetBuffer_t;
typedef std::list<packetBuffer_t> packetBuffer_list_t;

class TRB3Receiver : public artdaq::CommandableFragmentGenerator
{
  public:
	explicit TRB3Receiver(fhicl::ParameterSet const& ps);
	virtual ~TRB3Receiver();

  protected:
	// The "getNext_" function is used to implement user-specific
	// functionality; it's a mandatory override of the pure virtual
	// getNext_ function declared in CommandableFragmentGenerator

	bool         getNext_(artdaq::FragmentPtrs& output) override;
	void         start(void) override;
	virtual void start_();
	virtual void stop(void) override;
	virtual void stopNoMutex(void) override;

	virtual void ProcessData_(artdaq::FragmentPtrs& output);

	packetBuffer_list_t packetBuffers_;

	bool        rawOutput_;
	std::string rawPath_;

	// FHiCL-configurable variables. Note that the C++ variable names
	// are the FHiCL variable names with a "_" appended

	int         dataport_;
	std::string ip_;
	std::string command_ip_;
	int         commandport_;
	int         rcvbuf_;

	// Spill tracking
	int                           spill_timeout_ms_;
	artdaq::Fragment::timestamp_t spill_start_timestamp_;
	artdaq::FragmentPtr           held_fragment_;
	artdaq::FragmentPtrs          spill_fragments_;
	bool                          in_spill_;
	bool                          spill_verified_;
	uint32_t                      subrun_number_;

	// The packet number of the next packet. Used to discover dropped packets
	size_t expectedPacketNumber_;

	// Socket parameters
	struct sockaddr_in si_data_;
	int                datasocket_;
	struct sockaddr_in comm_si_data_;
	int                commandsocket_;

	bool configure_after_spill_;

  private:
	void receiveLoop_();

	std::unique_ptr<std::thread> receiverThread_;
	std::mutex                   receiveBufferLock_;
	packetBuffer_list_t          receiveBuffers_;

	// Number of milliseconds per fragment
	double                                fragmentWindow_;
	std::chrono::steady_clock::time_point lastFrag_;

	void sendTRB3Start() { sendTRB3Command("START:" + std::to_string(run_number())); }
	void sendTRB3Stop() { sendTRB3Command("STOP"); }
	void sendTRB3Configure() { sendTRB3Command("CONFIGURE"); }
	void sendTRB3Command(std::string command);
};
}  // namespace ots

#endif /* artdaq_demo_Generators_ToySimulator_hh */
