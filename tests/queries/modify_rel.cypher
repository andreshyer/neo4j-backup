MATCH (j:Person)
WHERE j.name = "Tom Hanks"
MATCH (j)-[p:ACTED_IN]-(m:Movie)
WHERE m.title = "Cast Away"

SET p.bool_example = false
SET p.born = 1956
SET p.int_example = 1
SET p.point_3d_example = point({x: 3.0, y: 0.0, z: 2.0, crs: 'cartesian-3d'})
SET p.localdatetime_example = datetime('2015-07-04T19:32:24.000000000+00:00')
SET p.date_example = date('1999-01-01')
SET p.point_2d_example = point({x: 3.0, y: 0.0, crs: 'cartesian'})
SET p.datetime_example = datetime('2015-06-24T12:50:35.556000000+01:00')
SET p.point_geo_3d_example = point({x: 56.0, y: 12.0, z: 2, crs: 'wgs-84-3d'})
SET p.duration_example = duration('P5M1DT12H')
SET p.odd_prop = "$time('21:40:32.142000000+01:00')"
SET p.name = "Tom Hanks"
SET p.localtime_example = time('12:50:35.556000000+00:00')
SET p.point_geo_2d_example = point({x: 56.0, y: 12.0, crs: 'wgs-84'})
SET p.float_example = 0.334
SET p.time_example = time('21:40:32.142000000+01:00')
SET p.array_example = [true, false]