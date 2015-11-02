package jobs;

import java.util.List;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.beust.jcommander.internal.Lists;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.CalendarDate;
import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.model.Shape;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import datastore.AgencyTx;
import datastore.GlobalTx;
import datastore.VersionedDataStore;
import models.Snapshot;
import models.transit.*;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.mapdb.Fun.Tuple2;
import play.Logger;
import scala.actors.threadpool.Arrays;
import utils.GeoUtils;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class ProcessGtfsSnapshotExportAll implements Runnable {
	private Collection<Tuple2<String, Integer>> snapshots;
	private File output;
	private LocalDate startDate;
	private LocalDate endDate;

	/** Export the named snapshots to GTFS */
	public ProcessGtfsSnapshotExportAll(Collection<Tuple2<String, Integer>> snapshots, File output, LocalDate startDate, LocalDate endDate) {
		this.snapshots = snapshots;
		this.output = output;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	/**
	 * Export the master branch of the named agencies to GTFS. The boolean variable can be either true or false, it is only to make this
	 * method have a different erasure from the other
	 */
	public ProcessGtfsSnapshotExportAll(Collection<String> agencies, File output, LocalDate startDate, LocalDate endDate, boolean isagency) {
		this.snapshots = Lists.newArrayList(agencies.size());

		for (String agency : agencies) {
			// leaving version null will cause master to be used
			this.snapshots.add(new Tuple2<String, Integer>(agency, null));
		}

		this.output = output;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	/**
	 * Export this snapshot to GTFS, using the validity range in the snapshot.
	 */
	public ProcessGtfsSnapshotExportAll (Snapshot snapshot, File output) {
		this(Arrays.asList(new Tuple2[] { snapshot.id }), output, snapshot.validFrom, snapshot.validTo);
	}

	@Override
	public void run() {
		GTFSFeed feed = new GTFSFeed();
		
		GlobalTx gtx = VersionedDataStore.getGlobalTx();
		AgencyTx atx = null;
		
		try {
			for (Tuple2<String, Integer> ssid : snapshots) {
				String agencyId = ssid.a;
				Agency agency = gtx.agencies.get(agencyId);
				com.conveyal.gtfs.model.Agency gtfsAgency = agency.toGtfs();
				Logger.info("Exporting agency %s", agencyId);

				if (ssid.b == null) {
					atx = VersionedDataStore.getAgencyTx(agencyId);
				}
				else {
					atx = VersionedDataStore.getAgencyTx(agencyId, ssid.b);
				}
				
				// write the agencies.txt entry
				feed.agency.put(agencyId, agency.toGtfs());

				Map<String, com.conveyal.gtfs.model.Route> gtfsRoutes = Maps.newHashMap();
				// // write the routes
				for (Route route : atx.routes.values()) {
					// only export approved routes
					if(true || route.status == StatusType.APPROVED) {
						com.conveyal.gtfs.model.Route gtfsRoute = route.toGtfs(gtfsAgency, gtx);

						feed.routes.put(route.getGtfsId(), gtfsRoute);

						gtfsRoutes.put(route.id, gtfsRoute);
					}
				}

				// EXPORTING ALL SHAPES BASED ON THE TRIP PATTERNS
				GeometryFactory gf = new GeometryFactory();
				for (TripPattern pattern : atx.tripPatterns.values()) {
					ServiceCalendar c = new ServiceCalendar();
					c.agencyId = pattern.agencyId;
					c.gtfsServiceId = pattern.id.toString() + "_service";
				    c.monday = true;
				    c.tuesday = true;
				    c.wednesday = true;
				    c.thursday = true;
				    c.friday = true;
				    c.saturday = true;
				    c.sunday = true;
				    com.conveyal.gtfs.model.Service gtfsService = c.toGtfs(toGtfsDate(startDate), toGtfsDate(endDate));
					// com.conveyal.gtfs.model.Service gtfsService = feed.getOrCreateService(pattern.id.toString() + "_service");  // creating a fake service


					Tuple2<String, Integer> nextKey = feed.shapePoints.ceilingKey(new Tuple2(pattern.id, null));
					if ((nextKey == null || !pattern.id.equals(nextKey.a)) && pattern.shape != null && !pattern.useStraightLineDistances) {
						// this shape has not yet been saved
						double[] coordDistances = GeoUtils.getCoordDistances(pattern.shape);

						for (int i = 0; i < coordDistances.length; i++) {
							Coordinate coord = pattern.shape.getCoordinateN(i);
							Shape shape = new Shape(pattern.id, coord.y, coord.x, i + 1, coordDistances[i]);
							feed.shapePoints.put(new Tuple2(pattern.id, shape.shape_pt_sequence), shape);
						}
					}

					// Generate a trip for each trip pattern
					com.conveyal.gtfs.model.Trip gtfsTrip = new com.conveyal.gtfs.model.Trip();
					Route route = atx.routes.get(pattern.routeId);
					com.conveyal.gtfs.model.Route gtfsRoute = gtfsRoutes.get(pattern.routeId);
					gtfsTrip.block_id = "";
					gtfsTrip.route = gtfsRoute;
					gtfsTrip.trip_id = pattern.id.toString() + "_trip";
					gtfsTrip.service = gtfsService;
					gtfsTrip.trip_headsign = pattern.headsign;
					gtfsTrip.trip_short_name = pattern.name;
					gtfsTrip.direction_id = 0;  // make a function based on the geo values
					if (pattern.shape != null && !pattern.useStraightLineDistances) {
						gtfsTrip.shape_id = pattern.id;
					}
					if (route.wheelchairBoarding != null) {
						if (route.wheelchairBoarding.equals(AttributeAvailabilityType.AVAILABLE)) {
							gtfsTrip.wheelchair_accessible = 1;
						} else if (route.wheelchairBoarding.equals(AttributeAvailabilityType.UNAVAILABLE)) {
							gtfsTrip.wheelchair_accessible = 2;
						}
					} else {
						gtfsTrip.wheelchair_accessible = 0; // unknown
					}
					feed.trips.put(gtfsTrip.trip_id, gtfsTrip);

					feed.services.put(gtfsService.service_id, gtfsService);

					// Generate the stoptimes for each trip pattern
					int stopSequence = 1;
					List<TripPatternStop> ps = pattern.patternStops;
					for (int i = 0; i < ps.size(); i++) {
    					TripPatternStop tps = ps.get(i);
    					Stop stop = atx.stops.get(tps.stopId);
    					com.conveyal.gtfs.model.StopTime gst = new com.conveyal.gtfs.model.StopTime();
    					gst.stop_headsign = stop.stopName;
						gst.stop_id = stop.getGtfsId(); 
						gst.stop_sequence = stopSequence++;
						gst.trip_id = gtfsTrip.trip_id;
						int a = (stopSequence-1) * 1800;
						gst.arrival_time = a;
						gst.departure_time = a+60;
						gst.shape_dist_traveled = tps.shapeDistTraveled;

						// write the stop as needed
						if (!feed.stops.containsKey(gst.stop_id)) {
							feed.stops.put(gst.stop_id, stop.toGtfs());
						}
    					feed.stop_times.put(new Tuple2(gtfsTrip.trip_id, gst.stop_sequence), gst);
    				}
    				// generate all the frequencies
					Frequency f = new Frequency();
					f.trip = gtfsTrip;
					f.start_time = 0;  // 00:00:00
					f.end_time = 86439;  // 23:59:59
					f.exact_times = 0;
					f.headway_secs = 3600;
					feed.frequencies.put(gtfsTrip.trip_id, f);
				}
			}

			Logger.info(output.getAbsolutePath().toString());
			feed.toFile(output.getAbsolutePath());
		} finally {
			gtx.rollbackIfOpen();
			if (atx != null) atx.rollbackIfOpen();
		}
	}
	
	public static int toGtfsDate (LocalDate date) {
		return date.getYear() * 10000 + date.getMonthOfYear() * 100 + date.getDayOfMonth();
	}
}

