#!/usr/bin/python

from __future__ import print_function

from datetime import datetime
import getopt
import logging
import numpy
import os
import re
import sys
import time

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "df:j:q:u:v", ["help", "output="])
	except getopt.GetoptError as err:
		# print help information and exit:
		print(err) # will print something like "option -a not recognized"
		sys.exit(2)

	file = None
	job = None
	user = None
	verbose = False
	queryKey = "NONE"
	DEBUG = False
	for o, a in opts:
		if o == "-j":
			job = a
		elif o == "-d":
			DEBUG = True
		elif o == "-f":
			file = a
		elif o == "-q":
			queryKey = a
		elif o == "-u":
			user = a
		elif o == "-v":
			verbose = True
		else:
			assert False, "unhandled option"

	if DEBUG:
		logging.basicConfig(level=logging.INFO)

	# Argument checking.
	if file == None and job == None:
		assert False, "Need -f or -j"

	# If a job, download to a temp file.
	if job != None:
		#file = tempfile.NamedTemporaryFile()
		file = "/tmp/tmpfile"
		command = "yarn logs -applicationId %s" % job
		if user != None:
			command = command + (" -appOwner %s" % user)
		command = command + " > %s" % file
		logging.info(command)
		os.system(command)

	# Regular expressions we use.
	containerIdRe = re.compile("^Container: (container_\d+_\d+_\d+_\d+)")
	vertexNameRe = re.compile("VertexName: ([^,]+)")
	bytesRe = re.compile("BYTES_READ=(\d+)")
	bytesWriteRe = re.compile("BYTES_WRITTEN=(\d+)")
	recordsRe = re.compile("INPUT_RECORDS_PROCESSED=(\d+)")
	exceptionRe = re.compile("([A-Za-z]+Exception)")
	timeRe = re.compile("(\d{4}-\d{2}-\d{2} \S+)")
	taskFinishedRe = re.compile("\[Event:TASK_FINISHED\]: vertexName=([^,]+)")
	taskCounterRe = re.compile("([A-Z_]+=\d+)")

	# Static strings.
	nonzeroString = "non-zero"
	syslogAttemptString = "syslog_attempt"
	taskCompleteString = "Task completed"
	dagFinishedString = "Event:DAG_FINISHED"

	# Tracked statistics.
	stats = {}
	globalStats = {}
	dagStats = {}
	counterStats = {}
	exceptions = {}
	resetStats(stats)
	blackout = True

	# Preamble
	if verbose:
		print("QueryKey,ContainerName,nBytes,nRecs,Duration,nException,nExit");

	# Parse the log file.
	fd = open(file)
	for line in fd:
		# Look for a new container.
		result = re.match(containerIdRe, line)
		if result != None:
			if stats["containerName"] != None:
				printStats(stats, verbose)
				resetStats(stats)
				blackout = True
			logging.info("Starting New Container:")
			stats["containerName"] = result.group(1)

		# Look for the vertex name.
		result = re.search(vertexNameRe, line)
		if result != None:
			vName = result.group(1)
			if stats["vertexName"] == None:
				logging.info("Vertex name set to " + vName)
				stats["vertexName"] = vName
			elif vName != stats["vertexName"]:
				cName = stats["containerName"]
				printStats(stats, verbose)
				resetStats(stats)
				logging.info("Changing vertex name to " + vName)
				stats["containerName"] = cName
				stats["vertexName"] = vName
			if globalStats.has_key(vName):
				globalStats[vName]["totalContainers"] += 1
			else:
				globalStats[vName] = {}
				globalStats[vName]["totalContainers"] = 1
				globalStats[vName]["totalBytes"] = 0
				globalStats[vName]["totalBytesWrite"] = 0
				globalStats[vName]["totalRecs"] = 0
				globalStats[vName]["byteObservations"] = []

		# Look for data read.
		reads = re.findall(bytesRe, line)
		if len(reads) > 0:
			found = sum([ int(x) for x in reads ])
			logging.info("Bytes " + str(found))
			stats["nBytes"] += found
			if stats.has_key("vertexName"):
				vName = stats["vertexName"]
				if globalStats.has_key(vName):
					logging.info("Add bytes to " + vName)
					globalStats[vName]["totalBytes"] += found
					globalStats[vName]["byteObservations"].extend([found])
			else:
				print("Error: Data read outside of named vertex")

		# Data Written.
		writes = re.findall(bytesWriteRe, line)
		if len(writes) > 0:
			found = sum([ int(x) for x in writes ])
			logging.info("Bytes Written " + str(found))
			stats["nBytesWrite"] += found
			if stats.has_key("vertexName"):
				vName = stats["vertexName"]
				if globalStats.has_key(vName):
					logging.info("Add bytes to " + vName)
					globalStats[vName]["totalBytesWrite"] += found
					#globalStats[vName]["byteObservations"].extend([found])
			else:
				print("Error: Data read outside of named vertex")

		# Look for records read.
		records = re.findall(recordsRe, line)
		if len(records) > 0:
			found = sum([ int(x) for x in reads ])
			logging.info("Records " + str(found))
			stats["nRecs"] += found
			if stats.has_key("vertexName"):
				vName = stats["vertexName"]
				if globalStats.has_key(vName):
					logging.info("Add records to " + vName)
					globalStats[vName]["totalRecs"] += found
			else:
				print("Error: Records read outside of named vertex")

		# Look for finished tasks.
		result = re.search(taskFinishedRe, line)
		if result != None:
			vName = result.group(1)

			# Extract task counters.
			logging.info("Extracting counters for " + vName)
			counters = re.findall(taskCounterRe, line)
			if not counterStats.has_key(vName):
				counterStats[vName] = {}
				counterStats[vName]["startTime"] = sys.maxint
				counterStats[vName]["finishTime"] = 0
			for c in counters:
				(key, val) = c.split('=')
				if not counterStats[vName].has_key(key):
					counterStats[vName][key] = 0
				counterStats[vName][key] += int(val)

			# Get start and end times.
			offset = line.find("startTime=")
			if offset == -1:
				logging.error("Could not find start time for " + vName)
			else:
				startTime = int(line[offset+10 : offset+23])
				counterStats[vName]["startTime"] = min(counterStats[vName]["startTime"], startTime)
			offset = line.find("finishTime=")
			if offset == -1:
				logging.error("Could not find end time for " + vName)
			else:
				finishTime = int(line[offset+11 : offset+24])
				counterStats[vName]["finishTime"] = max(counterStats[vName]["finishTime"], finishTime)

		# Look for exceptions.
		result = re.search(exceptionRe, line)
		if result != None:
			stats["nException"] += 1
			exception = result.group(1)
			vName = stats["vertexName"]
			if exceptions.has_key(vName):
				exceptions[vName][exception] = 1
			else:
				exceptions[vName] = {}
				exceptions[vName][exception] = 1

		# Look for non-zero exits.
		if line.find(nonzeroString) != -1:
			stats["nExit"] += 1

		# Look for DAG finished
		if line.find(dagFinishedString) != -1:
			# Capture the DAG start and end time.
			result = re.search("startTime=(\d+)", line)
			if result != None:
				dagStats["startTime"] = int(result.group(1))
			result = re.search("finishTime=(\d+)", line)
			if result != None:
				dagStats["finishTime"] = int(result.group(1))

	# Dump out the last container.
	printStats(stats, verbose)

	# Dump out global statistics.
	dumpGlobalStats(queryKey, globalStats, counterStats)

	# Dump out the counter stats.
	dumpCounterStats(queryKey, counterStats)

	# Compare times.
	dumpRuntimes(queryKey, globalStats, counterStats, dagStats)

	# Dump out exception info.
	dumpExceptionInfo(queryKey, exceptions)

	# Some key job statistics. Right now just intermediate data.
	print("\nOther Stats:")
	print("TotalIntermediateData")
	intermediateData = 0
	for vertex in globalStats.keys():
		if vertex[0:7] == "Reducer":
			intermediateData += globalStats[vertex]["totalBytes"]
	print(readable(intermediateData))

def dumpRuntimes(queryKey, globalStats, counterStats, dagStats):
	runTimes = []

	print("")
	for vertex in globalStats.keys():
		# Get runtime from the counter stats area.
		if counterStats.has_key(vertex):
			startTime = counterStats[vertex]["startTime"]
			finishTime = counterStats[vertex]["finishTime"]
			totalTime = finishTime - startTime
			runTimes.append(totalTime)
			print("%s Start %s Finish %s Total %f" % (vertex, toDateTime(startTime), toDateTime(finishTime), totalTime / 1000.0))

	totalVertexRuntime = sum(runTimes) / 1000.0
	dagStart = dagStats["startTime"]
	dagFinish = dagStats["finishTime"]
	dagRuntime = (dagStats["finishTime"] - dagStats["startTime"]) / 1000.0
	print("DAG Start %s Finish %s Total %f" % (toDateTime(dagStart), toDateTime(dagFinish), dagRuntime))
	print("Total Vertex Runtime = %fs" % totalVertexRuntime)

def toDateTime(timestamp):
	return datetime.fromtimestamp(float(timestamp)/1000).strftime("%Y-%m-%d %H:%M:%S")

def dumpExceptionInfo(queryKey, exceptions):
	print("\nPossible Errors:")
	print("QueryKey,Vertex,ListOfExceptions")
	for v in exceptions:
		vName = v
		if vName == None:
			vName = "AppMaster"
		print(queryKey, ",", vName, ",", end='', sep='')
		print(','.join(exceptions[v].keys()))

def dumpCounterStats(queryKey, counterStats):
	print("\nNotable Counter Stats:")
	importantCounterStats = ["DATA_LOCAL_TASKS", "RACK_LOCAL_TASKS", "SPILLED_RECORDS", \
	    "WRONG_MAP", "WRONG_REDUCE", "WRONG_LENGTH", "FAILED_SHUFFLE", \
	    "BAD_ID", "IO_ERROR"]
	print("QueryKey,Vertex", end='')
	for key in importantCounterStats:
		print(",", key, sep='', end='')
	print("")
	for vertex in counterStats.keys():
		print(queryKey, ",", vertex, sep='', end='')
		for key in importantCounterStats:
			if counterStats[vertex].has_key(key):
				print(",", counterStats[vertex][key], sep='', end='')
			else:
				print(",0", sep='', end='')
		print("")

def dumpGlobalStats(queryKey, globalStats, counterStats):
	print("\nQueryKey,Vertex,TotalContainers,TotalBytesRead,TotalRecsRead,TotalBytesWrite,ReadHistCounts,ReadHistCenters,RunTimeSec")
	for vertex in globalStats.keys():
		# Compute histograms if data is available.
		logging.info("Compute histograms for " + vertex)
		counts = []
		centers = []
		if globalStats[vertex]["byteObservations"] != []:
			(counts, centers) = numpy.histogram(globalStats[vertex]["byteObservations"])
			counts = counts.tolist()
			centers = [ readable(x) for x in centers.tolist() ]

		# Get runtime from the counter stats area.
		runTime = -1000
		if counterStats.has_key(vertex):
			runTime = counterStats[vertex]["finishTime"] - counterStats[vertex]["startTime"]

		print(queryKey, ",", \
		    vertex, ",", \
		    globalStats[vertex]["totalContainers"], ",", \
		    readable(globalStats[vertex]["totalBytes"]), ",", \
		    globalStats[vertex]["totalRecs"], ",", \
		    readable(globalStats[vertex]["totalBytesWrite"]), ",", \
		    str(counts), ",", \
		    str(centers), ",", \
		    runTime / 1000.0, \
		    sep='')

def readable(num):
	for x in ['bytes','KB','MB','GB','TB']:
		if num < 1024.0:
			return "%3.1f %s" % (num, x)
		num /= 1024.0

def resetStats(stats):
	stats["containerName"] = None
	stats["vertexName"] = None
	stats["nBytes"] = 0
	stats["nBytesWrite"] = 0
	stats["nRecs"] = 0
	stats["nException"] = 0
	stats["nExit"] = 0

def printStats(stats, verbose, queryKey="NONE"):
	if not verbose:
		return

	print(queryKey, ",", \
		stats["containerName"], ",", \
		stats["vertexName"], ",", \
		stats["nBytes"], ",", \
		stats["nRecs"], ",", \
		stats["nException"], ",", \
		stats["nExit"], sep='')

if __name__ == "__main__":
	main()
