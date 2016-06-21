/*
 * LearnerMH.h
 *
 *  Created on: Apr 15, 2016
 *      Author: gll
 */

#ifndef LEARNERMH_H_
#define LEARNERMH_H_

#include "handlers/PaxosMH.hpp"

using namespace boost;

namespace paxos
{

/**
 * Handles values persistence
 */
template<class PaxosListenerType> class LearnerMH : public PaxosMH<PaxosListenerType>
{
	typedef PaxosMH<PaxosListenerType> 		MH;
	typedef boost::shared_ptr<PaxosListenerType> 	paxos_listener_ptr_t;

	public:
		~LearnerMH(){};
		void init(paxos_listener_ptr_t listener){};
		std::string getXmlConfigurationTag();
		void onConsensus(const PaxosMessage& message);

	protected:
		void reset(uint32_t peerId ){};

	private:
		//vector<pair<paxosheaderType,valueType> >	mLearnedValues;

};

template<class PaxosListenerType> std::string LearnerMH<PaxosListenerType>::getXmlConfigurationTag()
 {
	 return XML_LEARNER_ID;
 }

template<class PaxosListenerType> void LearnerMH<PaxosListenerType>::onConsensus(const PaxosMessage& message)
{
	std::cout << "INBOUND " << MH::mId << " " << message << std::endl;
	//store value and notify the listener
}

}

#endif /* LEARNERMH_H_ */
