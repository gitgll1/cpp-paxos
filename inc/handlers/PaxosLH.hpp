/*
 * PaxosLH.h
 *
 *  Created on: Apr 7, 2016
 *      Author: gll
 */

#ifndef PAXOSLH_H_
#define PAXOSLH_H_

#include <map>
#include <set>
#include <stdint.h>
#include <string>
#include <memory>
#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/system/error_code.hpp>
#include <boost/thread.hpp>
#include "protocole/message.hpp"
#include "configuration/Configurator.h"
#include "handlers/roles/AcceptorMH.hpp"
#include "handlers/roles/ProposerMH.hpp"
#include "handlers/roles/LearnerMH.hpp"

#include <sys/time.h>

#define BUFFER_SIZE 1024

using namespace std;
using namespace boost;

namespace paxos
{

	typedef boost::shared_ptr<asio::io_service> 		io_service_ptr_t;
	typedef boost::shared_ptr<asio::ip::udp::socket> 	socket_ptr_t;//scoped_ptr
	typedef boost::shared_ptr<asio::deadline_timer> 	deadline_timer_ptr_t;

	const uint8_t STANBY_HEARTBEAT_COUNT = 3;

	/**
	 * Paxos Linehandler handles the routing of messages based on its associated handlers roles.
	 * The the time scheduling of the paxos phases (for the proposer role only) is also
	 * .
	 * The paxos algorithm is implemented in the handleReceive method and
	 * the different timers call back methods (onPhaseTimeOut(),...)
	 */
	template<class PaxosListenerType> class PaxosLH
	{
		typedef boost::shared_ptr<PaxosListenerType>		listener_ptr_t;

	public:
		PaxosLH(io_service_ptr_t io_service_ptr, boost::shared_ptr<PaxosListenerType> listener)
			: mpIOService(io_service_ptr),
			  mPort(0), mTTL(22),
			  mSocketSend(new asio::ip::udp::socket(*mpIOService)),
			  mSocketRcvd(new asio::ip::udp::socket(*mpIOService)),
			  mListener(listener),
			  hasProposer(false), hasAcceptor(false), hasLearner(false),
			  mPhaseTimeoutMs(250), mHeartbeatMs(10000), mLastMessageMs(0), mStandbyIdleTimeMs(0)
			{
				memset(mReadBuffer,0,sizeof(mReadBuffer));
			}

		~PaxosLH(){};

		void configure(const property_tree::ptree& configuration);
		void init();
		void start();
		void async_start();
		void stop();
		bool propose(const string& value);

	private:
		io_service_ptr_t 				mpIOService;
		short  							mPort;
		uint8_t 						mTTL;
		socket_ptr_t					mSocketSend;
		socket_ptr_t					mSocketRcvd;
		string  						mGroup;
		string 							mLocalAddr;
		char    						mReadBuffer[BUFFER_SIZE];
		asio::ip::udp::endpoint  		mMCAddr;
		asio::ip::udp::endpoint 		sender_endpoint_;
		AcceptorMH<PaxosListenerType> 	mAcceptor;
		ProposerMH<PaxosListenerType> 	mProposer;
		LearnerMH<PaxosListenerType> 	mLearner;
		listener_ptr_t				 	mListener;
		PaxosMessage 					mReceivedMessage;
		bool 							hasProposer;
		bool 							hasAcceptor;
		bool 							hasLearner;

		int 							mPhaseTimeoutMs	;
		int 							mHeartbeatMs;
		deadline_timer_ptr_t 			mProposerTimer;
		string 							mProposerId;
		long							mLastMessageMs;//millisecond is enough for heartbeat timeouts
		long							mStandbyIdleTimeMs;//used by standby timer to avoid resetting the timer with each received message

		void setProposerPhaseTimeOut();
		void setProposerHeartbeatTimeOut();
		void setProposerStandbyTimeOut();
		void postReceive();
		void handleReceive(const boost::system::error_code& error, std::size_t size);
		void onProposerPhaseTimeout(const boost::system::error_code& before_timeout);
		void onProposerHeartbeatTimeout(const boost::system::error_code& before_timeout);
		void onProposerStandbyTimeout(const boost::system::error_code& before_timeout);
		void send(const PaxosMessage& message);
		void send(const uint32_t decision, const MsgId msgId, const std::string& sender, const uint32_t proposal, const std::string& value );
		long getTimestamp();
	};

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::start()
{
	mStandbyIdleTimeMs = mHeartbeatMs;
	postReceive();
	if (hasProposer)
	{
		if (mProposer.isStartModeLeader())
		{
			send(mProposer.candidate());
			setProposerPhaseTimeOut();
		}
		else
		{
			mProposer.standby();
			setProposerStandbyTimeOut();
		}
	}
	mpIOService->run();
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::async_start()
{
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::stop()
{
	mSocketSend->close();
	mSocketRcvd->close();
}

template<class PaxosListenerType> inline bool PaxosLH<PaxosListenerType>::propose(const string& value)
{
	if (hasProposer && mProposer.isLeader())
	{
		send(mProposer.getPing());//dummy command not implemented in this release
		return true;
	}
	return false;

}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::configure(const boost::property_tree::ptree& configuration)
{
	try
	{
		mLocalAddr = configuration.get<std::string>(XML_INTERFACE);
		mGroup = configuration.get<std::string>(XML_GROUP);
		mPort = configuration.get<short>(XML_PORT);
		mTTL = configuration.get<uint8_t>(XML_TTL);
		std::cout << "PaxosLH(" <<mLocalAddr << "," << mGroup << ":" << mPort <<  ") is configured:" << std::endl;
		if (Configurator::isParameterSet(configuration, XML_PROPOSER_ID) )
		{
			mProposer.configure(configuration);
			hasProposer = true;
			mHeartbeatMs = configuration.get<int>(XML_PROPOSER_HEARTBEAT_MS);
			mPhaseTimeoutMs = configuration.get<int>(XML_PROPOSER_PHASE_TIMEOUT_MS);
			mProposerId = mProposer.getId();
		}
		if (Configurator::isParameterSet(configuration, XML_ACCEPTOR_ID) )
		{
			mAcceptor.configure(configuration);
			hasAcceptor = true;
		}
		if (Configurator::isParameterSet(configuration, XML_LEARNER_ID) )
		{
			mLearner.configure(configuration);
			hasLearner = true;
		}
	}
	catch (std::exception& e)
	{
		string xmlError = e.what();
		throw std::runtime_error("In configuration " + xmlError);
	}
}

template<class PaxosListenerType>  void PaxosLH<PaxosListenerType>::init()
{
	boost::asio::ip::udp::endpoint listen_address(boost::asio::ip::address::from_string(mLocalAddr),mPort);
	mMCAddr = boost::asio::ip::udp::endpoint(boost::asio::ip::address::from_string(mGroup),mPort);

	mSocketSend = socket_ptr_t(new boost::asio::ip::udp::socket(*mpIOService, mMCAddr.protocol()));
	mSocketSend->set_option(boost::asio::ip::multicast::hops(mTTL));

	boost::asio::ip::udp::endpoint listen_endpoint(boost::asio::ip::address::from_string(mLocalAddr), mPort);
	mSocketRcvd->open(listen_endpoint.protocol());
	mSocketRcvd->set_option(boost::asio::ip::udp::socket::reuse_address(true));
	mSocketRcvd->bind(listen_endpoint);
	mSocketRcvd->set_option(boost::asio::ip::multicast::join_group(boost::asio::ip::address::from_string(mGroup)));

	std::cout << "Paxos line handler is initialized with component(s):" << std::endl;
	if (hasProposer)
	{
		mProposerTimer = deadline_timer_ptr_t(new boost::asio::deadline_timer(*mpIOService, boost::posix_time::millisec(mHeartbeatMs)));//no async wait => dummy value
		mProposer.init(mListener);
	}
	if (hasAcceptor)
	{
		mAcceptor.init(mListener);
	}
	if (hasLearner)
	{
		mLearner.init(mListener);
	}
}

template<class PaxosListenerType> inline void PaxosLH<PaxosListenerType>::send( const uint32_t decision, const MsgId msgId, const std::string& sender, const uint32_t proposal, const std::string& value )
{
	char buffer[BUFFER_SIZE];//move to write buffer
	int len = sprintf ( buffer, "%d,%d,%s,%d,%s\n",decision,(uint32_t) msgId,sender.c_str(),proposal,value.c_str() );
	mSocketSend->send_to(boost::asio::buffer(buffer,len),mMCAddr);
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::setProposerPhaseTimeOut()
{
	if (mProposerTimer)
	{
		mProposerTimer->expires_from_now(boost::posix_time::millisec(mPhaseTimeoutMs));//TODO check if >0 to see if a phase is cancelled?
		mProposerTimer->async_wait(boost::bind(&PaxosLH::onProposerPhaseTimeout, this, boost::asio::placeholders::error));
	}
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::setProposerHeartbeatTimeOut()
{
	if (mProposerTimer)
	{
		mProposerTimer->expires_from_now(boost::posix_time::millisec(mHeartbeatMs));//TODO check if >0 to see if a phase is cancelled?
		mProposerTimer->async_wait(boost::bind(&PaxosLH::onProposerHeartbeatTimeout, this, boost::asio::placeholders::error));
	}
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::setProposerStandbyTimeOut()
{//restart an async wayt
	if (mProposerTimer)
	{
		if (mStandbyIdleTimeMs > 100 )//mHeartbeatMs/10  )//check hb > 10 in this case
		{//Optimization: We don't want to reset the timer with every message received
			mStandbyIdleTimeMs = 0;
			mProposerTimer->expires_from_now(boost::posix_time::millisec(mHeartbeatMs*STANBY_HEARTBEAT_COUNT));//TODO check if >0 to see if a phase is cancelled?
			mProposerTimer->async_wait(boost::bind(&PaxosLH::onProposerStandbyTimeout, this, boost::asio::placeholders::error));
		}
	}
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::postReceive()
{
	mSocketRcvd->async_receive_from(
			boost::asio::buffer(mReadBuffer),sender_endpoint_,
			boost::bind(&PaxosLH::handleReceive, this,
					boost::asio::placeholders::error,
					boost::asio::placeholders::bytes_transferred)
	);
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::handleReceive(const boost::system::error_code& error, std::size_t size)
{
	if (error)
	{
		std::cerr << "ERROR on receive " << error.message() << std::endl;
		stop();
		return;
	}
	mReadBuffer[size]=0;//end of string check max size
	mReceivedMessage.parse(mReadBuffer);
	if (mReceivedMessage.mSenderId != mProposerId)
	{
		long time = getTimestamp();
		mStandbyIdleTimeMs = time - mLastMessageMs;
		mLastMessageMs = time;
	}
	switch (mReceivedMessage.mMsgId)
	{
		case PREPARE_REQUEST:
			if (hasAcceptor) send(mAcceptor.replyPrepare(mReceivedMessage));
			if (hasProposer && mProposer.isStandby())
			{
				setProposerStandbyTimeOut();//there is still a proposer pinging the quorum
				mProposer.synchronize(mReceivedMessage.mDecisionId, mReceivedMessage.mProposal);
			}
			break;
		case ACCEPT_REQUEST:
			if (hasAcceptor) send(mAcceptor.replyAccept(mReceivedMessage));
			break;
		case PROMISE_REPLY:
			if (hasProposer)
			{
				send(mProposer.replyPromise(mReceivedMessage));
				if(mProposer.hasReachedQuorumMajority())
				{
					setProposerPhaseTimeOut();
				}
			}
			break;
		case ACCEPTED_VALUE:
			if (hasProposer)
			{
				send(mProposer.replyAccepted(mReceivedMessage));
				if(mProposer.hasLearnQuorum())
				{
					mListener->onConsensus(mReceivedMessage.mDecisionId, mReceivedMessage.mValue);
					mProposer.doEndOfCycle();
					if (mProposer.isLeader())
					{
						setProposerHeartbeatTimeOut();
					} else {
						setProposerStandbyTimeOut();
					}
				}
			}
			break;
		case CONSENSUS_NOTIFICATION:
			if (!hasProposer)
			{
				mListener->onConsensus(mReceivedMessage.mDecisionId, mReceivedMessage.mValue);
			}
			else
			{
				if (mProposer.isCandidate())
				{
					mProposer.standby();//lost election
					setProposerStandbyTimeOut();
				}
			}
			if (hasLearner)  mLearner.onConsensus(mReceivedMessage);
			break;
		case REJECT_REPLY:
			if (hasProposer)
			{
				if (mReceivedMessage.mValue != mProposer.getId() )//to allow e.g. a primary configured re-start after with leader already running
				{
					mProposer.standby();
					setProposerStandbyTimeOut();
					uint32_t decisionId = mReceivedMessage.mDecisionId + 1;
					mProposer.synchronize(decisionId,mReceivedMessage.mProposal);
				}
				else
				{
					send(mProposer.replyReject(mReceivedMessage));
				}
			}
			break;
		default:
			//paxos requests are ignored. Log an error for other types //	LOG_ERROR ( mId << " received UNKNOWN message Id " );
			break;
	}
	postReceive();
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::onProposerPhaseTimeout(const boost::system::error_code& before_timeout)
 {
	 if (!before_timeout)//<=> realtimeout
	 {
		 switch (mProposer.getPendingAcceptorMessageType())
		 {
			 case PROMISE_REPLY:
				send(mProposer.candidate());//resend increasing proposalID
				setProposerPhaseTimeOut();//for next accept
				 break;
			 case ACCEPTED_VALUE:
				setProposerPhaseTimeOut();//election is lost restart cycle
				mProposer.doEndOfCycle();
				 break;
			 case NULL_MESSAGE:
			//	 cout << "PHASE TIMEOUT WITH NO MESSAGE REPLY EXPECTED" << endl;
				 break;
			 default:
				 break;
		 }
	 }
	 else if (before_timeout != boost::asio::error::operation_aborted)
	 {
		 std::cerr << " Unexpected error "  << before_timeout.message() << std::endl;
	 }
 }

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::onProposerHeartbeatTimeout(const boost::system::error_code& before_timeout)
{
	 if (!before_timeout)//<=> realtimeout
	 {
		{
			send(mProposer.getPrepareRequest());
			setProposerPhaseTimeOut();//next request
		}
	 }
	 else if (before_timeout != boost::asio::error::operation_aborted)
	 {
		 std::cerr << " Heartbeat sleep interrupted with unexpected error "  << before_timeout.message() << std::endl;
	 }
}

template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::onProposerStandbyTimeout(const boost::system::error_code& before_timeout)
{
	 if (!before_timeout)//<=> realtimeout
	 {
		{
			send(mProposer.candidate());
			setProposerPhaseTimeOut();//next request
		}
	 }
	 else if (before_timeout != boost::asio::error::operation_aborted)
	 {//check state aborted or chancel and then general case
		 std::cerr << " Heartbeat sleep interrupted with unexpected error "  << before_timeout.message() << std::endl;
	 }
}


template<class PaxosListenerType> void PaxosLH<PaxosListenerType>::send(const PaxosMessage& message)
{
	if (message.mMsgId != NULL_MESSAGE)
	{
		//cout << "OUTBOUND[" << message.mSenderId << "] = " << message << std::endl;
		send(message.mDecisionId,message.mMsgId,message.mSenderId, message.mProposal, message.mValue);
	}
}

template<class PaxosListenerType> inline long PaxosLH<PaxosListenerType>::getTimestamp()
{
	struct timeval tp;
	gettimeofday(&tp, NULL);
	return tp.tv_sec * 1000 + tp.tv_usec / 1000;
}

}/* namespace paxos */

#endif /* PAXOSLH_H_ */
