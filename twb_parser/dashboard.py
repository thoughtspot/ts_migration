import pandas as pd 
import numpy as np
from utilities.logger_config import get_logger

class Dashboard():
    def __init__(self):
        pass  
        def __init__(self):
                pass          

    def find_dashboard(self, soup, twb_name):
        dashboard_name = []
        data_dict = []
        
        get_logger().info(f'Dashboard parsing started for {twb_name}')
        # Define the columns upfront
        ordered_col = ['twb file name', 'object type', 'datasource name', 'table name',	'remote column name', 'local column name',
                        'worksheet name', 'dashboard name', 'property type',	'property name', 'property value', 'proposed value',
                        'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details']
        
        try:
            # Attempt to find dashboards
            dashboards_tag = soup.find('dashboards')
            
            if dashboards_tag is None:
                # If 'dashboards' tag is not found, raise an AttributeError
                # raise AttributeError("'dashboards' tag not found in the soup")
                get_logger().info(f'Dashboard not available for {twb_name}')
            
            dashboard_soup = dashboards_tag.find_all('dashboard')
                    
            for i in dashboard_soup:
                dashboard_name.append(i.get('name'))
                data_dict.append({
                    'object type': 'dashboard',
                    'dashboard name': i.get('name')
                })
            
            # Create DataFrame from the extracted data
            dashboard_df = pd.DataFrame.from_dict(data_dict)
            
            # Add missing columns with NaN values
            for col in ordered_col:
                if col not in dashboard_df.columns:
                    dashboard_df[col] = np.NaN
            
            dashboard_df['twb file name'] = twb_name
            
            # Reorder the columns according to the order defined
            dashboard_df = dashboard_df.reindex(columns=ordered_col)
            dashboard_df.reset_index(drop=True, inplace=True)
            
        except AttributeError as e:
            
            get_logger().error(f'Attribute Error in find_dashboard: {e}')

            # Handle the AttributeError and log the error
            print("Error: Dashboard not available. " + str(e))
            dashboard_df = pd.DataFrame(columns=ordered_col)
            dashboard_df.loc[0, 'exec_error_details'] = str(e)
            dashboard_df.loc[0, 'object type'] = 'dashboard'
            dashboard_df['twb file name'] = twb_name
        
        except Exception as e:

            get_logger().error(f'Exception Error in find_dashboard: {e}')
            # Handle all other exceptions and log the error
            print("An unexpected error occurred: " + str(e))
            dashboard_df = pd.DataFrame(columns=ordered_col)
            dashboard_df.loc[0, 'exec_error_details'] = str(e)
            dashboard_df.loc[0, 'object type'] = 'dashboard'
            dashboard_df['twb file name'] = twb_name
        
        finally:
            pass
        get_logger().info(f'Dashboard parsing completed for {twb_name}')
        return dashboard_df
