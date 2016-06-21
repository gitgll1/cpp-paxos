/*
 * AcceptorMH.h
 *
 *  Created on: Apr 7, 2016
 *      Author: gll
 */

#ifndef ACCEPTORMH_H_
#define ACCEPTORMH_H_

#include <boost/system/error_code.hpp>
#include "handlers/PaxosMH.hpp"

using namespace std;

namespace paxos
{

	template<class PaxosListenerType> class AcceptorMH : public PaxosMH<PaxosListenerType>
	{
		static const int8_t MAX_WRONG_VALUE_COUNT = 4;//this is a timeout on accept reply (set e.g. to 2xNumberOfProposers to avoid races between proposers)
		typedef PaxosMH<PaxosListenerType> MH;
		typedef boost::shared_ptr<PaxosListenerType> paxos_listener_ptr_t;

		public:
			~AcceptorMH(){};
			PaxosMessage replyPrepare(const PaxosMessage& message);
			PaxosMessage replyAccept(const PaxosMessage& message);
			void init(paxos_listener_ptr_t listener);
			string getXmlConfigurationTag();

		protected:
			void reset(uint32_t peerId );

		private:
			PaxosMessage       mReply;
			uint32_t           mLastAcceptedProposalId;
			uint32_t           mLastPromisedProposalId;
			string			   mLastSenderId;
			string             mAcceptedValue;
			uint8_t 		   mWrongValueCount;

	};

template<class PaxosListenerType> inline PaxosMessage AcceptorMH<PaxosListenerType>::replyPrepare(const PaxosMessage& message)
{
	mReply.init();
	MH::logInbound(message);
	if (!MH::isSenderBehind(message, mLastPromisedProposalId))
	{
		if (mAcceptedValue == ACCEPTED_VALUE_INIT  )
		{
			string value = ACCEPTED_VALUE_INIT;
			mReply.mDecisionId = MH::mDecisionId;
			mReply.mMsgId = PROMISE_REPLY;
			mReply.mSenderId = MH::mId;
			mReply.mProposal = message.mProposal;
			mReply.mValue = message.mValue;//possible optimization use init like vev for other than heartbeat
			mLastPromisedProposalId = message.mProposal;
			mLastSenderId = message.mSenderId;
		}
		else
		{
			cerr << "\tValue is wrong: Received=" << mReply.mValue << " - Expected=" << mAcceptedValue << " => message is dropped." << endl;
			mWrongValueCount++;
			if (mWrongValueCount > MAX_WRONG_VALUE_COUNT)
			{
				mAcceptedValue = ACCEPTED_VALUE_INIT;
				mWrongValueCount = 0;
				cout << "\tReached max wrong value timeout: Switched back accept valute to init value in order to allow new promise reply" << endl;
			}
		}
	}
	else
	{//reject to notify the proposer about current status
		cerr << "DecisionID is behind: Expected=" << MH::mDecisionId << " => Sending a reject reply." << endl;
		mReply.mDecisionId = MH::mDecisionId;
		mReply.mMsgId = REJECT_REPLY;
		mReply.mSenderId = MH::mId;
		mReply.mProposal = mLastPromisedProposalId;//or last accepted?
		mReply.mValue = mAcceptedValue;
	}
	return mReply;
}

template<class PaxosListenerType> inline PaxosMessage AcceptorMH<PaxosListenerType>::replyAccept(const PaxosMessage& message)
{
	mReply.init();
	MH::logInbound(message);

	if (!PaxosMH<PaxosListenerType>::isSenderBehind(message, mLastPromisedProposalId))
	{
		uint32_t proposal = message.mProposal;
		if (proposal == mLastPromisedProposalId && mLastSenderId == message.mSenderId)
		{
			mLastAcceptedProposalId = proposal;
			mAcceptedValue = message.mValue;//compare with cached value drop message if wrong
			mReply.mDecisionId = message.mDecisionId;
			mReply.mMsgId = ACCEPTED_VALUE;
			mReply.mSenderId = MH::mId;
			mReply.mProposal = mLastAcceptedProposalId;
			mReply.mValue    = mAcceptedValue;
			mWrongValueCount = 0;
		}
	}
	else
	{
		cerr << "Sender is behind or senderId is wrong, message is dropped" << std::cout;
	}
	return mReply;
}

template<class PaxosListenerType> std::string AcceptorMH<PaxosListenerType>::getXmlConfigurationTag()
{
	return XML_ACCEPTOR_ID;
}

template<class PaxosListenerType> void AcceptorMH<PaxosListenerType>::init(paxos_listener_ptr_t listener)
{
	MH::init(listener);
	mAcceptedValue = ACCEPTED_VALUE_INIT;
	mLastAcceptedProposalId = 0;
	mLastPromisedProposalId = 0;

	cout << "\t" << MH::mId << " is initiallized" << endl;
}

template<class PaxosListenerType> inline void AcceptorMH<PaxosListenerType>::reset(uint32_t peerId )
{
	MH::mDecisionId = peerId;
	mAcceptedValue = ACCEPTED_VALUE_INIT;
	mLastPromisedProposalId = 0;
	mLastAcceptedProposalId = 0;
}

}

#endif /* ACCEPTORMH_H_ */
