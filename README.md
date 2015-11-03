gtfs-editor
===========

A web-based GTFS editing framework.

*Requires*

	Play Framework 1.2.x (does not work with Play 2.x)
	Postgresql 9.1+ 
	PostGIS 1.5+

*Install*

Follow the instructions [here](INSTALL.md) for information on installation.

*Fork Notes [WIP]*

This fork contains an endpoint and class to export all GTFS of trip patterns that don't have calendars or frequencies set yet.

It is still a work in progress:

- [ ] Too many trips are generated between stops, should only be generated when there's a defined shape in between the stops
- [ ] The agency id cleansing (removing collons) should be cleaned up itself
- [ ] The actual times between stops should be gotten from the trip pattern if set. Or it should be calculated based on a fixed speed and the distance between the stops
 
