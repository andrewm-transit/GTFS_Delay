import pandas as pd
import geopandas as gpd
import partridge as ptg
import partridge.gtfs
import shapely.ops as shpo
import shapely as shp


def daytype(day):
    if day.weekday() == 6:
        return "sun"
    elif day.weekday() == 5:
        return "sat"
    else:
        return "wkd"


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
        self.seg_speed_df = None
        self.seg_time_df = None

        self._snap_stops()
        self._create_segments()
        self._calc_segment_speeds()
        self._create_segment_time_df()

    def _snap_stops(self) -> None:
        """Snaps stops to the route shape."""
        self.stops = self.gtfs.stops

        # Filter stops for only stops that have stop_time data - this prevents segments from having incalculable speeds
        stops_with_stoptimes = self.gtfs.stop_times.dropna(subset=['arrival_time', 'departure_time'])['stop_id'] \
            .unique()
        self.stops = self.stops[self.stops['stop_id'].isin(stops_with_stoptimes)]

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
        # Order stops by linear reference position
        self.stops = self.stops.sort_values(by='lin_ref_pos')

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
        self.stop_pairs['seg_geom'] = self.stop_pairs[['lin_ref_pos_start', 'lin_ref_pos_end']] \
            .apply(lambda x: shpo.substring(self.gtfs.shapes['geometry'].iloc[0],
                                            x.iloc[0],
                                            x.iloc[1],
                                            normalized=True), axis=1)

        # Make GeoDataFrame
        self.stop_pairs = gpd.GeoDataFrame(data=self.stop_pairs,
                                           geometry='seg_geom',
                                           crs='EPSG:4326')

        # Calculate distance in miles
        if self.epsg is None:
            self.epsg = 6561  # Oregon State Plane South Feet
        self.stop_pairs['seg_distance'] = self.stop_pairs.to_crs(epsg=self.epsg)['seg_geom'] \
                                              .apply(lambda x: shp.length(x)) / 5280

    def _calc_segment_speeds(self):
        """ Takes stop-pairs and uses stop_times to create new table with data from both"""

        self.seg_speed_df = pd.DataFrame(columns=['trip_id',
                                                  'seg_id',
                                                  'start_stop',
                                                  'end_stop',
                                                  'start_time',
                                                  'end_time',
                                                  'seg_distance',
                                                  'seg_speed'])

        for trip in self.gtfs.stop_times['trip_id'].unique():

            for _, stop_pair in self.stop_pairs.iterrows():
                start_time = self.gtfs.stop_times[(self.gtfs.stop_times['trip_id'] == trip) &
                                                  (self.gtfs.stop_times['stop_id'] == stop_pair['start_stop'])][
                    'departure_time'].values[0]
                end_time = self.gtfs.stop_times[(self.gtfs.stop_times['trip_id'] == trip) &
                                                (self.gtfs.stop_times['stop_id'] == stop_pair['end_stop'])][
                    'arrival_time'].values[0]
                trip_time = (end_time - start_time) / 60 / 60

                stop_pair_df = pd.DataFrame({'trip_id': trip,
                                             'seg_id': stop_pair['name'],
                                             'start_stop': stop_pair['start_stop'],
                                             'end_stop': stop_pair['end_stop'],
                                             'start_time': start_time / 60 / 60,
                                             'end_time': end_time / 60 / 60,
                                             'seg_distance': stop_pair['seg_distance'],
                                             'seg_speed': stop_pair['seg_distance'] / trip_time
                                             },
                                            index=[0])
                self.seg_speed_df = pd.concat([self.seg_speed_df, stop_pair_df], ignore_index=True)

    def _create_segment_time_df(self):
        """ Creates a table with data from gtfs.stop_times and stop_pairs for graphing"""
        self.seg_time_df = self.seg_speed_df

        # Create a new final point at the end of the line to graph last segment
        for trip_id in self.seg_time_df['trip_id'].unique():
            trip_data = self.seg_time_df[self.seg_time_df['trip_id'] == trip_id]
            last_row = trip_data.iloc[len(trip_data) - 1, :]
            new_last_row = pd.DataFrame({'trip_id': last_row['trip_id'],
                                         'seg_id': 'final',
                                         'start_stop': last_row['end_stop'],
                                         'end_stop': last_row['end_stop'],
                                         'start_time': last_row['end_time'],
                                         'end_time': last_row['end_time'],
                                         'seg_distance': last_row['seg_distance'],
                                         'seg_speed': 0},
                                        index=[0])
            self.seg_time_df = pd.concat([self.seg_time_df, new_last_row], ignore_index=True)

        self.seg_time_df = pd.merge(left=self.seg_time_df,
                                    right=self.stop_pairs[['start_stop', 'lin_ref_pos_start']],
                                    how='left')

        # Set final positions as end of linear reference line
        self.seg_time_df.loc[self.seg_time_df['lin_ref_pos_start'].isna(), 'lin_ref_pos_start'] = 1

        # Merge data from trips and calendar_dates or calendar to get day type
        # TODO: Make this work with calendar.txt and calendar_dates.txt
        self.seg_time_df = pd.merge(left=self.seg_time_df,
                                    right=self.gtfs.trips[['trip_id', 'service_id']],
                                    on='trip_id')
        self.seg_time_df = pd.merge(left=self.seg_time_df,
                                    right=self.gtfs.calendar_dates[['service_id', 'date']],
                                    on='service_id')

        # Apply daytype function to get actual day
        # TODO: Holiday filtering - daytype doesn't match service_id type
        self.seg_time_df['day_type'] = self.seg_time_df['date'].apply(daytype)


