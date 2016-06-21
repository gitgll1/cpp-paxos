/*
 * Configurator.h
 *
 *  Created on: Apr 7, 2016
 *      Author: gll
 */

#ifndef CONFIGURATOR_H_
#define CONFIGURATOR_H_

#include <iostream>
#include <boost/noncopyable.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

using namespace std;
using namespace boost;

namespace paxos
{

	const string XML_PROPOSER_ID = "paxos_service.line_handler.proposer.id";
	const string XML_PROPOSER_START_STATE = "paxos_service.line_handler.proposer.start_state";
	const string XML_PROPOSER_HEARTBEAT_MS = "paxos_service.line_handler.proposer.heartbeat_ms";
	const string XML_PROPOSER_PHASE_TIMEOUT_MS = "paxos_service.line_handler.proposer.phase_timeout_ms";
	const string XML_ACCEPTOR_ID = "paxos_service.line_handler.acceptor.id";
	const string XML_LEARNER_ID = "paxos_service.line_handler.learner.id";
//	const string XML_ACCEPTOR_DISCARD_PREPARE_COUNT = "paxos_service.line_handler.acceptor.discard_prepare_count";
	const string XML_INTERFACE = "paxos_service.line_handler.interface";
	const string XML_GROUP = "paxos_service.line_handler.group";
	const string XML_PORT = "paxos_service.line_handler.port";
	const string XML_TTL = "paxos_service.line_handler.ttl";
	const string XML_QUORUM = "paxos_service.quorum";

	class Configurator : private noncopyable
	{

		public:
			virtual ~Configurator();

			static property_tree::ptree load(const string& filename)
			{
				boost::property_tree::ptree configuration;
				boost::property_tree::read_xml(filename, configuration);
				return configuration;
			}

			static bool isParameterSet(const property_tree::ptree& ptree, const string& name )
			{
				namespace pt = property_tree;

				optional< string> value = ptree.get_optional<string>( name );
				if(( !value ))
					return false;
				return true;
			}

	};

} /* namespace paxos */

#endif /* CONFIGURATOR_H_ */
