/*
 * PaxosMH.h
 *
 *  Created on: Apr 15, 2016
 *      Author: gll
 */

#ifndef PAXOSMH_H_
#define PAXOSMH_H_

#include <boost/shared_ptr.hpp>
#include "configuration/Configurator.h"

using namespace std;
using namespace boost;

namespace paxos
{

#define PAXOS_PROPOSER_ID   "-proposer"
#define PAXOS_ACCEPTOR_ID   "-acceptor"

template<class PaxosListenerType> class PaxosMH
{
	typedef boost::shared_ptr<PaxosListenerType> paxos_listener_ptr_t;

public:
	PaxosMH() : mDecisionId(0), mTrace(false) {};
	virtual ~PaxosMH(){};

	virtual string getXmlConfigurationTag() = 0;

	virtual void init(paxos_listener_ptr_t listener);
	virtual void configure(const property_tree::ptree& configuration);
	string getId() const;
	void logInbound(const PaxosMessage& message);


protected:
	string					mId;
	uint32_t				mDecisionId;
	paxos_listener_ptr_t 	mListener;
	bool					mTrace;


	virtual void reset(uint32_t peerId ) = 0;

	bool isSenderBehind(const PaxosMessage& message, uint32_t proposalId)
	{
		if (message.mDecisionId < mDecisionId)
		{
			return true;
		}
		if ( message.mDecisionId > mDecisionId )
		{
			reset (message.mDecisionId);
		}
		else if (message.mProposal < proposalId)
		{
			cerr << "\tProposalId is behind: Expected >= " << proposalId << " => message is dropped." << endl;
			return true;
		}
		return false;
	}

};

template<class PaxosListenerType> void PaxosMH<PaxosListenerType>::init(paxos_listener_ptr_t listener)
{
	mDecisionId = 0;
	mListener = listener;
	mListener->onStateChange(mId,INITIAL);
}

template<class PaxosListenerType> void PaxosMH<PaxosListenerType>::configure(const property_tree::ptree& configuration)
 {
   		try
   		{
   			mId = configuration.get<std::string>(getXmlConfigurationTag());
   			cout << "\t" << mId << " is configured" << endl;
   		}
   		catch (std::exception& e)
   		{
   			std::string xmlError = e.what();
   			throw std::runtime_error("In configuration " + xmlError);
   		}
}

template<class PaxosListenerType> inline string  PaxosMH<PaxosListenerType>::getId() const
	{
		return mId;
	}

template<class PaxosListenerType> inline void PaxosMH<PaxosListenerType>::logInbound(const PaxosMessage& message)
	{
	 if (mTrace)	std::cout << "INBOUND [" << mId << "] = " <<  message << std::endl;
	}
}

#endif /* PAXOSMH_H_ */
