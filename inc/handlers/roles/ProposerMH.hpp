/*
 * ProposerMH.h
 *
 *  Created on: Apr 7, 2016
 *      Author: gll
 */

#ifndef PROPOSERMH_H_
#define PROPOSERMH_H_

#include <set>
#include <map>
#include <boost/foreach.hpp>
#include "handlers/PaxosMH.hpp"

using namespace std;
using namespace boost;

namespace paxos
{

	template<class PaxosListenerType> class ProposerMH : public PaxosMH<PaxosListenerType>
	{
		typedef map<string, set<string> >	StringSetMap;
		typedef set<string> ListOfUniqueStrings;
		typedef PaxosMH<PaxosListenerType> MH;
		typedef boost::shared_ptr<PaxosListenerType> paxos_listener_ptr_t;

		public:
			~ProposerMH() {};
			PaxosMessage replyPromise(const PaxosMessage& message);
			PaxosMessage replyAccepted(const PaxosMessage& message);
			PaxosMessage replyReject(const PaxosMessage& message);
			PaxosMessage getPrepareRequest();
			bool belowQuorumMajority();
			bool hasReachedQuorumMajority();
			bool belowLearnQuorum ();
			bool hasLearnQuorum ();
			MsgId getPendingAcceptorMessageType();
			void doEndOfCycle();
			void synchronize(uint32_t decisionId, uint32_t proposalId);

			void init(paxos_listener_ptr_t listener);
			std::string getXmlConfigurationTag();
			void configure(const property_tree::ptree& cf);

			bool isStartModeLeader() {return mStartState == LEAD_PRIMARY;}
			bool isCandidate() {return mState == LEAD_CANDIDATE;}
			bool isLeader() {return mState == LEAD_PRIMARY;}
			bool isStandby() {return mState == LEAD_STANDBY;}
			PaxosMessage candidate()
			{
				handleStateTransition(LEAD_CANDIDATE);
				return getPrepareRequest();
			}
			void standby() {handleStateTransition(LEAD_STANDBY);}

		protected:
			void reset(uint32_t peerId );

		private:
			PaxosMessage 				mReply;
			MsgId						mPendingAcceptorMessageType;
			set<string>  				mQuorumSet;
			uint32_t        			mQuorumMajority;
			uint32_t        			mLastProposedNumber; // number which we last proposed
			string          			mCurrLeader;
			string          			mAcceptedValue;
			string          			mPromotedValue; // the value which we promote
			set<string> 				mAcceptorsPositive; // acceptors which accepted our proposal
			map<string, set<string> >	mLearnedValues;
			ProposerState 				mStartState;
			ProposerState 				mState;

			void clearVote();
			void handleStateTransition(ProposerState newState);

};

template<class PaxosListenerType> inline PaxosMessage ProposerMH<PaxosListenerType>::replyPromise(const PaxosMessage& message)
{
	mReply.init();
	MH::logInbound(message);

	if (!MH::isSenderBehind(message, mLastProposedNumber))
	{
		if (message.mProposal == mLastProposedNumber)
		{
			mAcceptorsPositive.insert(message.mSenderId);
			if (belowQuorumMajority())
			{
				mPendingAcceptorMessageType = PROMISE_REPLY;//still waiting
			}
			else if (hasReachedQuorumMajority())
			{
				if (MH::mTrace) cout << "REACHED PROMISE QUORUM" << endl;
				if ((isCandidate() || isLeader()))
				{
					mReply.mMsgId = ACCEPT_REQUEST;
					mReply.mDecisionId = MH::mDecisionId;
					mReply.mSenderId = MH::mId;
					mReply.mProposal = mLastProposedNumber;
					mReply.mValue = mPromotedValue;
					mPendingAcceptorMessageType = ACCEPTED_VALUE;
				}
			}
		}
	}
	return mReply;
}

template<class PaxosListenerType> inline PaxosMessage ProposerMH<PaxosListenerType>::replyAccepted(const PaxosMessage& message)
{
	mReply.init();
	MH::logInbound(message);
	if (!MH::isSenderBehind(message, mLastProposedNumber))
	{
		string value = message.mValue;
		if (value != ACCEPTED_VALUE_INIT)//must be checked against proposedValue!
		{
			string senderId = message.mSenderId;
			map<string, set<string> >::iterator found = mLearnedValues.find(value);
			if (found == mLearnedValues.end())
			{
				set<string> newSet;
				newSet.insert(senderId);
				mLearnedValues[value] = newSet;
			}
			else
			{//found
				found->second.insert(senderId);
			}
			if (belowLearnQuorum())
			{
				mPendingAcceptorMessageType = ACCEPTED_VALUE;//still waiting
			}
			if (hasLearnQuorum())
			{

				if (MH::mTrace)  cout << "REACHED ACCEPT QUORUM: ELECTED PROPOSER=" << mCurrLeader << endl;
				if (mCurrLeader == MH::mId)
				{
					mReply.mDecisionId = MH::mDecisionId;
					mReply.mMsgId = CONSENSUS_NOTIFICATION;
					mReply.mSenderId = MH::mId;
					mReply.mProposal = mLastProposedNumber;
					mReply.mValue = mPromotedValue;
					handleStateTransition(LEAD_PRIMARY);
					mPendingAcceptorMessageType = NULL_MESSAGE;
				}
			}
		}
	}
	return mReply;
}

template<class PaxosListenerType> inline PaxosMessage ProposerMH<PaxosListenerType>::replyReject(const PaxosMessage& message)
{
	mReply.init();
	MH::logInbound(message);
	std::cout << "Received reject: Re-synchronizing to current decision/proposal ids" << std::endl;
	reset(message.mDecisionId);
	mLastProposedNumber = message.mProposal;
	return mReply;
}

template<class PaxosListenerType> inline PaxosMessage ProposerMH<PaxosListenerType>::getPrepareRequest()
{
	if (MH::mTrace) cout << endl;
	mLastProposedNumber++;
	clearVote();
	mReply.mDecisionId = MH::mDecisionId;
	mReply.mMsgId = PREPARE_REQUEST;
	mReply.mSenderId = MH::mId;
	mReply.mProposal = mLastProposedNumber;
	mReply.mValue = ACCEPTED_VALUE_INIT;
	mPendingAcceptorMessageType = PROMISE_REPLY;
	return mReply;
}

template<class PaxosListenerType> inline bool ProposerMH<PaxosListenerType>::hasReachedQuorumMajority()
{
	bool reachedQuorum = (mAcceptorsPositive.size() == mQuorumMajority);////exact count (first hit) to avoid multiples send accept
	//if (reachedQuorum) cout << "REACHED PROMISE QUORUM" << endl;
	return reachedQuorum;
}

template<class PaxosListenerType> inline bool ProposerMH<PaxosListenerType>::belowQuorumMajority()
{
	return mAcceptorsPositive.size() < mQuorumMajority;
}

template<class PaxosListenerType> inline bool ProposerMH<PaxosListenerType>::hasLearnQuorum ()
{
		StringSetMap::iterator i;
		mCurrLeader = "";
		for ( i = mLearnedValues.begin(); i != mLearnedValues.end(); i++ )
		{
			if (i->second.size() == mQuorumMajority) //only notify once
			{
				mCurrLeader = i->first;
				ListOfUniqueStrings::iterator primary = i->second.find(mCurrLeader + PAXOS_ACCEPTOR_ID);
				break;
			}
		}

		return (!mCurrLeader.empty());
}

template<class PaxosListenerType> inline bool ProposerMH<PaxosListenerType>::belowLearnQuorum ()
{
	bool isBelow = true;
		StringSetMap::iterator i;
		mCurrLeader = "";
		for ( i = mLearnedValues.begin(); i != mLearnedValues.end(); i++ )
		{
			if (i->second.size() >= mQuorumMajority) //only notify once
			{
				isBelow = false;
			}
		}
		return isBelow;
}

template<class PaxosListenerType> inline void ProposerMH<PaxosListenerType>::doEndOfCycle()
		{
			if (MH::mTrace) cout << "PROCESSING END OF CYCLE#"  << MH::mDecisionId << endl;
			MH::mDecisionId++;
			mAcceptedValue = ACCEPTED_VALUE_INIT;
			clearVote();
			mLearnedValues.clear();
			mCurrLeader = "";
			mLastProposedNumber = 0;
			mPendingAcceptorMessageType = NULL_MESSAGE;

}

template<class PaxosListenerType> inline MsgId ProposerMH<PaxosListenerType>::getPendingAcceptorMessageType()
{
	return mPendingAcceptorMessageType;
}

template<class PaxosListenerType> std::string ProposerMH<PaxosListenerType>::getXmlConfigurationTag()
{
	return XML_PROPOSER_ID;
}

template<class PaxosListenerType> void ProposerMH<PaxosListenerType>::init(paxos_listener_ptr_t listener)
{
	MH::init(listener);
	mLastProposedNumber=0;
	mPromotedValue = MH::mId;
	mAcceptedValue = ACCEPTED_VALUE_INIT;
	mState = INITIAL;
	mPendingAcceptorMessageType = NULL_MESSAGE;
	cout << "\t" << MH::mId << " is initiallized" << endl;
}

template<class PaxosListenerType> void ProposerMH<PaxosListenerType>::configure(const property_tree::ptree& cf)
{
	try
	{
		PaxosMH<PaxosListenerType>::configure(cf);
		BOOST_FOREACH(property_tree::ptree::value_type const& v, cf.get_child(XML_QUORUM))
		{
			//mQuorumSet.insert(v.second.get("<xmlattr>.id", ""));
			string value = v.second.get("<xmlattr>.id", "");
			if (value.size() > 0) mQuorumSet.insert(value);
		}
		cout << "\tQuorum: " << mQuorumSet.size() << " acceptors [ " ;
		BOOST_FOREACH(string const& v, mQuorumSet)
		{
			cout << v << " ";
		}
		mQuorumMajority = (int)(mQuorumSet.size() / 2) + 1;
		cout << "] - Majority=" << mQuorumMajority << endl;
		mPromotedValue = cf.get<std::string>(XML_PROPOSER_START_STATE);
		cout << "\t" << MH::mId << " starting mode=" << mPromotedValue << endl;
		if (mPromotedValue == "PRIMARY") mStartState = LEAD_PRIMARY;
		else mStartState = LEAD_STANDBY;
	}
	catch (std::exception& e)
	{
		string xmlError = e.what();
		throw std::runtime_error("In configuration " + xmlError);
	}
}

template<class PaxosListenerType> inline void ProposerMH<PaxosListenerType>::clearVote()
{
	mAcceptorsPositive.clear();
}

template<class PaxosListenerType> inline void ProposerMH<PaxosListenerType>::reset ( uint32_t peerId )
{
	if (MH::mTrace) cout << "RESET DECISION_ID FROM "  << MH::mDecisionId << " TO " << peerId << endl;
	mAcceptedValue = ACCEPTED_VALUE_INIT;
	mLearnedValues.clear();
	mCurrLeader = "";
	MH::mDecisionId = peerId;
};

template<class PaxosListenerType> void ProposerMH<PaxosListenerType>::synchronize(uint32_t decisionId, uint32_t proposalId)
{
	MH::mDecisionId = decisionId;
	mLastProposedNumber = proposalId;
}

template<class PaxosListenerType> void ProposerMH<PaxosListenerType>::handleStateTransition(ProposerState newState)
{
	if (mState != newState)
	{
		mState = newState;
		MH::mListener->onStateChange(MH::mId,newState);
	}
}

} //paxos

#endif /* PROPOSERMH_H_ */
