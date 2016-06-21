//============================================================================
// Name        : paxos-light.cpp
// Author      : gll
// Version     :
// Copyright   : Your copyright notice
//============================================================================

#include <iostream>
#include "PaxosService.hpp"
#include "configuration/Configurator.h"

using namespace std;

class MyPaxosListener
{
public:
	void onStateChange(const string& id, const paxos::ProposerState state)
	{
		std::cout << "[" << id << "] TRANSITION TO STATE " << state << std::endl;
	}

	void onConsensus(const uint32_t decisionId, const std::string& acceptedValue)
	{
		std::cout << "CONSENSUS#" << decisionId << " Value=" << acceptedValue << std::endl;
	}

};

typedef paxos::PaxosService<MyPaxosListener> px_service;

int main(int argc, char* argv[])
{

	if (argc != 2)
	{
		std::cerr << "Usage: paxos <configuration_filename.xml>\n";
	    return 1;
	}

	paxos::io_service_ptr_t 				io_service_ptr(new boost::asio::io_service);
	boost::shared_ptr<MyPaxosListener> 		listener(new MyPaxosListener);

	try
	{
		px_service paxos_service(io_service_ptr, listener,paxos::Configurator::load(argv[1]));
		paxos_service.start();
	}
	catch (std::exception& e)
	{
		std::cerr << "Error while starting paxos_service: " << e.what() << std::endl;
	}

	std::cout << "paxos_service is stopped." << std::endl;
	return 0;
}
