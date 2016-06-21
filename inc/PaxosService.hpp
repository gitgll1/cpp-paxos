/*
 * DefaultPaxosListener.h
 *
 *  Created on: Apr 19, 2016
 *      Author: gll
 */

#ifndef DEFAULTPAXOSLISTENER_H_
#define DEFAULTPAXOSLISTENER_H_

#include "handlers/PaxosLH.hpp"

namespace paxos
{

/**
 ListenerType must implement the 2 following methods:
	- void onStateChange(string id, ProposerState state)
	- void onConsensus(uint32_t decisionId, const std::string acceptedValue)
 */
template <class ListenerType> class PaxosService
{

public:

	PaxosService(io_service_ptr_t io_service, boost::shared_ptr<ListenerType> listener, const boost::property_tree::ptree& configuration)
				: _lineHandler(io_service, listener)
	{
		_lineHandler.configure(configuration);
		_lineHandler.init();
	};

	void start()
	{
		_lineHandler.start();
	}

	void stop()
	{
		_lineHandler.stop();
	}

	bool propose(std::string value)
	{
		return _lineHandler.propose(value);
	}

private:
	PaxosLH<ListenerType> _lineHandler;
};

}

#endif /* DEFAULTPAXOSLISTENER_H_ */
