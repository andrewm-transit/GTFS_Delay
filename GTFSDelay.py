import pandas as pd
import geopandas as gpd
import partridge as ptg
import partridge.gtfs
import shapely.ops as shpo
import shapely as shp

class GTFSDelay:
    def __init__(self, gtfs: partridge.gtfs.Feed = None, epsg: str = None) -> None:
        """
        Initializes object

        Attributes:
            gtfs: A partridge GTFS file containing a single shape_id's gtfs data
            epsg
        """

        # TODO: Add error checking on gtfs to ensure that later functions execute cleanly

        self.gtfs = gtfs
        self.epsg = epsg

        self._snap_stops()
        self._create_segments()

    def _snap_stops(self) -> None:
        """Snaps stops to the route shape."""
        self.stops = self.gtfs.stops

        # Locate the 'snapped' point on the route geometry for each stop
        self.stops['snapped_geom'] = [shpo.nearest_points(self.gtfs.shapes['geometry'], x)[0][0]
                                      for x in self.stops['geometry']]

        # Find lin_ref_pos for each stop on route
        self.stops['lin_ref_pos'] = 0.0
        for i in range(len(self.stops)):
            self.stops.iloc[i, 12] = self.gtfs.shapes['geometry'][0]. \
                project(self.stops['snapped_geom'].iloc[i], normalized=True)

    def _create_segments(self) -> None:
        """Creates stop-pairs for the given """
        # Generate stop pairs
        self.stop_pairs = pd.DataFrame(columns=['name', 'start_stop', 'end_stop',
                                                'lin_ref_pos_start', 'lin_ref_pos_end'])
        for i in range(len(self.stops) - 1):
            start = self.stops['stop_id'].iloc[i]
            end = self.stops['stop_id'].iloc[i + 1]
            name = str(start) + '-' + str(end)
            self.stop_pairs = pd.concat([self.stop_pairs,
                                         pd.DataFrame({'name': name,
                                                       'start_stop': start,
                                                       'end_stop': end,
                                                       'lin_ref_pos_start': self.stops['lin_ref_pos'].iloc[i],
                                                       'lin_ref_pos_end': self.stops['lin_ref_pos'].iloc[i + 1]},
                                                      index=[0])],
                                        ignore_index=True)

        # Create line segment for each stop pair
        self.stop_pairs['seg_geom'] = self.stop_pairs[['lin_ref_pos_start', 'lin_ref_pos_end']]\
            .apply(lambda x: shpo.substring(self.gtfs.shapes['geometry'].iloc[0],
                                            x[0],
                                            x[1],
                                            normalized=True), axis=1)

        # Make GeoDataFrame
        self.stop_pairs = gpd.GeoDataFrame(data=self.stop_pairs,
                                           geometry='seg_geom',
                                           crs='EPSG:4326')

        # Calculate distance in miles
        if self.epsg is None:
            self.epsg = 6561  # Oregon State Plane South Feet
        self.stop_pairs['seg_distance'] = self.stop_pairs.to_crs(epsg=self.epsg)['seg_geom']\
                                              .apply(lambda x: shp.length(x)) / 5280
