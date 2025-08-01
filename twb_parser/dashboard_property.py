import numpy as np 
import pandas as pd 
from utilities.logger_config import get_logger

class Dashboard_property():

    def __init__(self):
        pass

    # For Image/Web-Page:
    def image(self,soup):
        try:
            get_logger().info('Image parsing started')
            # Create empty list for the required columns for Feasibility Report :
            dashboard_name=[]
            property_name=[]

            dashboard_soup=soup.find('dashboards').find_all('dashboard')
            for i in dashboard_soup :
                for j in i.find_all('zones'):
                    if j is not None:
                        for k in j.find_all('zone'):
                            if k is not None:    
                                property_name.append(k.get('type-v2'))
                                dashboard_name.append(i.get('name'))
                            else:
                                get_logger().info(f'Image not available in the dashboard: {i.get("name")}')
                                property_name.append('None')
                                dashboard_name.append(i.get('name'))
                    else:
                        get_logger().info(f'Image not available in the dashboard: {i.get("name")}')
                        property_name.append('None')
                        dashboard_name.append(i.get('name'))
            # Create an empty dataframe :                        
            image_df = pd.DataFrame()
            image_df['dashboard name']=dashboard_name
            image_df['worksheet name']=None
            image_df['property type']=None
            image_df['property value']=None
            image_df['property name']=property_name
            # Filter out the parameters for Image and WebPage & duplicate check :
            image_df = image_df[(image_df['property name']=='web') | (image_df['property name']=='bitmap') | (image_df['property name']=='empty')] 
            image_df.reset_index(drop=True , inplace=True)
            # Assigning the property type as per the mapping sheet :
            for i in range(len(image_df)):
                if image_df['property name'][i]=='bitmap':
                    image_df['property value'][i]='Image'
                elif image_df['property name'][i]=='web':
                    image_df['property value'][i]='Web-Page'
                elif image_df['property name'][i]=='empty':
                    image_df['property value'][i]='Empty Area'
                else:
                    pass
            image_df=image_df[['dashboard name','worksheet name','property type','property name','property value']]
            
        except Exception as e:
            get_logger().error(f'image not available: {str(e)}')
            
        return image_df

    # For Button :
    def button(self,soup):
        try:
            get_logger().info('Button parsing started')
            # create empty list to get the button details :
            button_type=[]
            button_name=[]
            button_action=[]
            property_type=[]
            dashboard_name=[]
            # check for buttons available in the dashboard :
            dashboard_soup=soup.find('dashboards').find_all('dashboard')
            try:
                for i in dashboard_soup :
                    for j in i.find_all('button'):
                        button_type.append(j.get('button-type'))
                        if j.find('caption') is not None:
                            button_name.append(j.find('caption').text)
                        else:
                            button_name.append('None')
                        button_action.append(j.get('action'))
                        property_type.append('Button')
                        dashboard_name.append(i.get('name'))
            except Exception as e:
                get_logger().info(f'Dashboard/Button details not available')

            # Create an empty dataframe :        
            button_df=pd.DataFrame()
            button_df['dashboard name']=dashboard_name
            button_df['worksheet name']=None
            button_df['property type']=None
            button_df['button_type']=button_type
            button_df['property value']=button_name
            button_df['action']=button_action
            button_df['property name']='button'
            # Create a concat column to check for the duplicates :
            button_df['concat']=button_df['button_type'].astype(str)+button_df['property value'].astype(str)+button_df['action'].astype(str)
            button_df.drop_duplicates(subset=['concat'],inplace=True)
            button_df.drop(columns=['concat'], inplace=True)
            button_df.reset_index(drop=True, inplace=True)
            button_df = button_df[['dashboard name','worksheet name','property type','property name','property value']]

            # If any button is available with no caption then it is a symbol button - like cross (X) :
            for i in range(len(button_df)):
                if ((button_df['property type'][i]=='Button') & (button_df['property value'][i]=='None')):
                    button_df['property value'][i]='Symbol'
                    
        except Exception as e:
            get_logger().error(f'Error in identifying button: {str(e)}')            
        
        return button_df

    # For container :
    def container(self,soup):
        try:
            get_logger().info('Container parsing started')
            # Create empty list as per teh feasibility report column :
            dashboard_name=[]
            chart_name=[]
            layout_id=[]
            parameter=[]
            type=[]
            x_axis=[]
            y_axis=[]

            # check for buttons available in the dashboard :
            dashboard_soup=soup.find('dashboards').find_all('dashboard')
            
            for i in dashboard_soup :
                for j in i.find_all('zone'):
                    if j is not None:
                        layout_id.append(j.get('layout-strategy-id'))
                        chart_name.append(j.get('name'))
                        parameter.append(j.get('param'))
                        type.append(j.get('type-v2'))
                        x_axis.append(j.get('x'))
                        y_axis.append(j.get('y'))
                        dashboard_name.append(i.get('name'))
                    else:
                        get_logger().info(f'Container not available in the dashboard: {i.get("name")}')
                        layout_id.append('None')
                        chart_name.append('None')
                        parameter.append('None')
                        type.append('None')
                        x_axis.append('None')
                        y_axis.append('None')
                        dashboard_name.append(i.get('name'))
            
            # Create an empty dataframe:
            df_cntnr=pd.DataFrame()
            df_cntnr['dashboard name']=dashboard_name
            df_cntnr['worksheet name']=None
            df_cntnr['chart_type']=chart_name
            df_cntnr['layout_id']=layout_id
            df_cntnr['parameter']=parameter
            df_cntnr['type']=type
            df_cntnr['x_axis']=x_axis
            df_cntnr['y_axis']=y_axis

            # For Vertical Containers , the charts have fixed X-axis and for Horizontal Containers , the charts have fixed Y-axis :
            x_cooordinate=None
            y_cooordinate=None
            try:
                for i in range(len(df_cntnr)):
                    if df_cntnr['layout_id'][i]=='distribute-evenly':
                        if df_cntnr['parameter'][i]=='vert':
                            x_cooordinate=df_cntnr['x_axis'][i]
                    if df_cntnr['layout_id'][i]=='distribute-evenly':
                        if df_cntnr['parameter'][i]=='horz':
                            y_cooordinate=df_cntnr['y_axis'][i]
            except Exception as e:
                get_logger().info(f'cordinate not available')
                        
            # vertical container df :
            df_vertical=df_cntnr[df_cntnr['x_axis']==x_cooordinate]
            df_vertical['concat']=df_vertical['dashboard name'].astype(str)+df_vertical['chart_type'].astype(str)+df_vertical['x_axis'].astype(str)+df_vertical['y_axis'].astype(str)
            df_vertical.drop_duplicates(subset=['concat'], inplace=True)
            df_vertical.reset_index(drop=True, inplace=True)
            df_vertical.drop(columns=['concat'], inplace=True)
        
            # Assigning the values in layout and parameter column :
            try:
                if len(df_vertical)> 0:
                    if (df_vertical['layout_id'].unique()[0] is not None) & (df_vertical['layout_id'].unique()[0] is not None) :
                        df_vertical['layout_id']=df_vertical['layout_id'].unique()[0]
                        df_vertical['parameter']=df_vertical['parameter'].unique()[0]
            except Exception as e:
                get_logger().info(f'layout values not available')

            # Horizontal container df :
            df_horizontal=df_cntnr[df_cntnr['y_axis']==y_cooordinate]
            df_horizontal['concat']=df_horizontal['dashboard name'].astype(str)+df_horizontal['chart_type'].astype(str)+df_horizontal['x_axis'].astype(str)+df_horizontal['y_axis'].astype(str)
            df_horizontal.drop_duplicates(subset=['concat'], inplace=True)
            df_horizontal.reset_index(drop=True, inplace=True)
            df_horizontal.drop(columns=['concat'], inplace=True)

            # Assigning the values in layout and parameter column :
            try:
                if len(df_horizontal)> 0:
                    if (df_horizontal['layout_id'].unique()[0] is not None) & (df_horizontal['layout_id'].unique()[0] is not None) :
                        df_horizontal['layout_id']=df_horizontal['layout_id'].unique()[0]
                        df_horizontal['parameter']=df_horizontal['parameter'].unique()[0]
            except Exception as e:
                get_logger().error(f'Error occured in horizontal containers: {str(e)}')

            # Combine Both Horizontal and vertical Containers :
            df_horz_vert=pd.concat([df_vertical, df_horizontal], ignore_index=True, sort=False)
            df_horz_vert.dropna(subset=['chart_type'], inplace=True)
            # Create columns as per teh feasibility report :
            df_horz_vert['property name']=None
            df_horz_vert['property type']=None
            df_horz_vert['property value']=None
            #print(df_horz_vert)
            df_horz_vert.reset_index(drop=True, inplace=True)

            for i in range(len(df_horz_vert)):
                df_horz_vert['property type'][i]=df_horz_vert['chart_type'][i]
                df_horz_vert['property name'][i]=df_horz_vert['parameter'][i]
                if df_horz_vert['parameter'][i]=='vert':
                    df_horz_vert['property value']='Vertical Container'
                
                if df_horz_vert['parameter'][i]=='horz':
                    df_horz_vert['property value']='Horizontal Container'


            df_horz_vert=df_horz_vert[['dashboard name','worksheet name','property type','property name','property value']]
        
        except Exception as e:
            get_logger().error(f'Error in container: {str(e)}')
        return df_horz_vert

    # For Text/Summary :
    def formatted_text(self,soup):
        try:
            # Create the empty list :
            dashboard_name=[]
            text_value=[]
            type=[]
            dashboard_soup=soup.find('dashboards').find_all('dashboard')
            for i in dashboard_soup :
                j=i.find('zone',{"type-v2":"text"})
                
                if j is not None:
                    for k in j.find_all('run'):
                        text_value.append(k.text)
                        dashboard_name.append(i.get('name'))
                        type.append(j.get('type-v2'))
                else:
                    get_logger().info('Formatted_text not available in the dashboard')    
                    text_value.append('None')
                    dashboard_name.append(i.get('name'))
                    type.append('None')
                
            # Create the dataframe :
            df_text = pd.DataFrame()
            df_text['dashboard name']=dashboard_name
            df_text['worksheet name']=None
            df_text['property value']=text_value
            df_text['property type']='text'
            df_text['property name']='formatted-text'
            df_text=df_text[(df_text['property value']!='Æ\n') & (df_text['property value']!='Æ ')]
            df_text.reset_index(drop=True, inplace=True)

            df_text=df_text[['dashboard name','worksheet name','property type','property name','property value']]
        except Exception as e:
            get_logger().error(f'Error in formatted text: {str(e)}')
        
        return df_text

    # For Sizing :
    def layout(self,soup):
        try:
            get_logger().info('Dashboard layout parsing started')
            # Create empty list for columns to be fetched from twb :
            dashboard_name=[]
            sizing_mode=[]
                    
            dashboard_soup=soup.find('dashboards').find_all('dashboard')

            for i in dashboard_soup :
                for j in i.find_all('size'):
                    if j is not None:
                        sizing_mode.append(j.get('sizing-mode'))
                        dashboard_name.append(i.get('name'))
                    else:
                        get_logger().info(f'Dashboard layout not available in the dashboard: {i.get("name")}')
                        sizing_mode.append('None')
                        dashboard_name.append(i.get('name'))
                                            
            # Create layout df :
            df_sizing=pd.DataFrame()
            df_sizing['dashboard name']=dashboard_name
            df_sizing['worksheet name']=None
            df_sizing['property value']=sizing_mode
            df_sizing['property type']='style'
            df_sizing['property name']='layout'
            # Exclude the device layout property which is appearing as a duplicate :
            df_sizing=df_sizing[df_sizing['property value']!='vscroll']
            df_sizing.dropna(subset=['property value'], inplace=True)
            df_sizing.reset_index(drop=True, inplace=True)        
            df_sizing=df_sizing[['dashboard name','worksheet name','property type','property name','property value']]
            get_logger().info('Dashboard layout parsing completed')
        except Exception as e :
            get_logger().error(f'Error in dashboard layout: {str(e)}')
            
        return df_sizing

    # For chart inside dashboard layout properties:
    def layout_property(self,soup):
        try:
            get_logger().info('Dashboard layout parsing started')
            dashboard_name=[]
            sizing_mode=[]
            chart_name=[]
            height=[]
            width=[]
            x_axis=[]
            y_axis=[]
            border_color=[]
            border_style=[]
            border_width=[]
            margin=[]
            # Dashboard soup :
            dashboard_soup=soup.find('dashboards').find_all('dashboard')
            try:
                for i in dashboard_soup :
                    for j in i.find_all('zone'):
                        if j is not None:
                            dashboard_name.append(i.get('name'))
                            border_color_val = None
                            border_style_val = None
                            border_width_val = None
                            margin_val = None

                            for k in j.find_all('zone-style'):
                                if k is not None:
                                    for format_tag in k.find_all('format'):
                                        attr = format_tag.get('attr')
                                        value = format_tag.get('value')
                                        if attr == 'border-color':
                                            border_color_val = value
                                        elif attr == 'border-style':
                                            border_style_val = value
                                        elif attr == 'border-width':
                                            border_width_val = value
                                        elif attr == 'margin':
                                            margin_val = value
                                else:
                                    get_logger().info(f'Zone/Zone styles not available in the dashboard: {i.get("name")}')
                                    dashboard_name.append(i.get('name'))
                                    border_color_val = None
                                    border_style_val = None
                                    border_width_val = None
                                    margin_val = None
                        else:
                            get_logger().info(f'Zone/Zone styles not available in the dashboard: {i.get("name")}')
                            dashboard_name.append(i.get('name'))
                            border_color_val = None
                            border_style_val = None
                            border_width_val = None
                            margin_val = None
                        
                        # Append the values or None if not found
                        border_color.append(border_color_val)
                        border_style.append(border_style_val)
                        border_width.append(border_width_val)
                        margin.append(margin_val)

                        chart_name.append(j.get('name'))
                        height.append(j.get('h'))
                        width.append(j.get('w'))
                        x_axis.append(j.get('x'))
                        y_axis.append(j.get('y'))
            except Exception as e:
                get_logger().error(f'Error in identifying Zone/Zone styles: {str(e)}')
                # print(e)

            # Create an empty dataframe :
            df_dashboard_layout=pd.DataFrame()
            df_dashboard_layout['worksheet name']=chart_name
            df_dashboard_layout['height']=height
            df_dashboard_layout['width']=width
            df_dashboard_layout['x_axis']=x_axis
            df_dashboard_layout['y_axis']=y_axis
            df_dashboard_layout['border_color']=border_color
            df_dashboard_layout['border_style']=border_style
            df_dashboard_layout['border_width']=border_width
            df_dashboard_layout['margin']=margin
            df_dashboard_layout['dashboard name']=dashboard_name

            # # Drop the null values and duplicates :
            df_dashboard_layout.dropna(subset=['worksheet name'], inplace=True)
            df_dashboard_layout.drop_duplicates(subset=['worksheet name'], inplace=True)
            df_dashboard_layout.reset_index(drop=True, inplace=True)

            # # Unpivot the dataframe :
            df_dashboard_layout= pd.melt(df_dashboard_layout, id_vars=['worksheet name','dashboard name'], value_vars=['height', 'width','x_axis',
                                                    'y_axis','border_color','border_style','border_width','margin'])

            df_dashboard_layout.sort_values(by =['worksheet name','variable'], inplace=True)
            df_dashboard_layout.reset_index(drop=True, inplace=True)
            df_dashboard_layout.rename(columns={
                                    'variable':'property name',
                                    'value':'property value'} , inplace=True)

            df_dashboard_layout['property type']='zone-style'
            #df_dashboard_layout['dashboard name']=dashboard_name

            df_dashboard_layout=df_dashboard_layout[['dashboard name','worksheet name','property type','property name','property value']]
            get_logger().info('Dashboard layout parsing completed')
        except Exception as e:
            get_logger().error(f'Error in dashboard layout: {str(e)}')
            print(e)
            
        return df_dashboard_layout

    ####
    # For Dashboard level Filters :
    def dashboard_level_filter(self,soup):
        try:
            get_logger().info('Dashboard level filter parsing started')
            dashboard_name=[]
            chart_name=[]
            type_v2=[]
            dashboard_soup=soup.find('dashboards').find_all('dashboard')

            for i in dashboard_soup :
                k=i.find('zone',{"type-v2":"filter"})
                if k is not None:
                    chart_name.append(k.get('name'))
                    dashboard_name.append(i.get('name'))
                    type_v2.append(k.get('type-v2'))
                else:
                    get_logger().info(f'Dashboard filter not available in the dashboard: {i.get("name")}')
                    chart_name.append('None')
                    dashboard_name.append(i.get('name'))
                    type_v2.append('None')
                    pass
                
            dashboard_filter=pd.DataFrame()
            dashboard_filter['dashboard name']=dashboard_name
            dashboard_filter['worksheet name']=None
            dashboard_filter['property type']='filter'
            dashboard_filter['property name']='Dashboard filter'
            dashboard_filter['property value']=chart_name
            
            dashboard_filter['unique_index_value']=None
            unique_chart_cols = dashboard_filter['property value'].unique()
            index_value = {value: index + 1 for index, value in enumerate(unique_chart_cols)}
            dashboard_filter['unique_index_value'] = dashboard_filter['property value'].map(index_value)
            dashboard_filter['unique_index_value']=dashboard_filter['unique_index_value'].astype(str)

            for i in range(len(dashboard_filter)):
                dashboard_filter['property type'][i]=dashboard_filter['property type'][i]+'_'+dashboard_filter['unique_index_value'][i]
                
            dashboard_filter.drop(columns=['unique_index_value'], inplace=True)

            dashboard_filter=dashboard_filter[['dashboard name','worksheet name','property type','property name','property value']]

            dashboard_filter.fillna('',inplace=True)
            get_logger().info('Dashboard level filter parsing completed')
        except Exception as e:
            get_logger().error(f'Error in dashboard level filter: {str(e)}')
            
        return dashboard_filter

    # Combine all Dashboard Objects :
    def dashboard_combined(self,image_df,button_df,df_horz_vert,df_text,df_sizing,df_dashboard_layout,dashboard_filter):
        try:
            df_dashboard_combine=pd.concat([image_df, button_df,df_horz_vert,df_text,df_sizing,df_dashboard_layout,dashboard_filter], ignore_index=True, sort=False)

            df_dashboard_combine['object type']=None
            df_dashboard_combine['datasource name']=None
            df_dashboard_combine['table name']=None
            df_dashboard_combine['remote column name']=None
            df_dashboard_combine['local column name']=None
            df_dashboard_combine['data type']=None
            df_dashboard_combine['calculation']=None
            df_dashboard_combine['conversion in TS']=None
            df_dashboard_combine['proposed value'] = None
            

            df_dashboard_combine=df_dashboard_combine[['object type','worksheet name','datasource name', 'dashboard name',
                'table name','remote column name','local column name','data type','property type','property name',
                'property value','proposed value','calculation','conversion in TS']]
            
        except Exception as e:
            get_logger().error(f'Error in dashboard combined: {str(e)}')
            
        return df_dashboard_combine

    # Dashboard Property:
    def dashboard_property(self,soup,dashboard_mapping_df, twb_name):
        get_logger().info(f'Dashboard parsing started for {twb_name}')
        ordered_col = ['twb file name', 'object type', 'datasource name', 'table name',	'remote column name', 'local column name', 
        'worksheet name', 'dashboard name', 'property type',	'property name', 'property value', 'conversion in TS', 'supported in TS', 'supported in Migrator', 'exec_error_details']
        try:    
            dashboards_tag = soup.find('dashboards')

            if dashboards_tag is None:
                raise AttributeError("'dashboards' tag not found in the soup")

            # call the functions :        
            image_df=self.image(soup)
            button_df=self.button(soup)
            df_horz_vert=self.container(soup)
            df_sizing=self.layout(soup)
            df_dashboard_layout=self.layout_property(soup)
            df_text=self.formatted_text(soup)
            df_sizing=self.layout(soup)
            dashboard_filter=self.dashboard_level_filter(soup)
            df_dashboard_combine=self.dashboard_combined(image_df,button_df,df_horz_vert,df_sizing,df_dashboard_layout,df_text,dashboard_filter)

                        
            for i in range(len(df_dashboard_combine)):
                df_dashboard_combine['object type'][i]='dashboard property'

                
            # # Merge the filter data df and mapping df :
            df_dashboard_property=pd.merge(df_dashboard_combine , dashboard_mapping_df,how='left',
                                left_on = ['property name'],
                                right_on = ['mapping key'])

            df_dashboard_property=df_dashboard_property[['object type',
                                                    'worksheet name','dashboard name','table name','remote column name','local column name','property type',
                                                    'property name','property value','proposed value','conversion in TS','supported in TS','supported in Migrator']]
            
            df_dashboard_property.sort_values('property type', inplace=True)
            df_dashboard_property.fillna('', inplace=True)


            ordered_col = ['twb file name', 'object type', 'datasource name', 'table name',	'remote column name', 'local column name',
                        'worksheet name', 'dashboard name', 'property type',	'property name', 'property value', 'proposed value','conversion in TS', 'supported in TS', 'supported in Migrator','exec_error_details']
            col = ['datasource name','exec_error_details']
            df_dashboard_property[col] = np.NaN
            df_dashboard_property['twb file name'] = twb_name
            df_dashboard_property.reset_index(drop=True, inplace=True)
            df_dashboard_property = df_dashboard_property.reindex(columns=ordered_col,)
        except AttributeError as e:

            get_logger().error(f'Attribute Error in dashboard_property: {str(e)}')
            error_data = {
                'twb file name': twb_name,
                'object type': 'dashboard property',
                'exec_error_details': 'Dashboard not available. ' + str(e)
            }
            df_dashboard_property = pd.DataFrame([error_data])
            df_dashboard_property = df_dashboard_property.reindex(columns=[
                'twb file name', 'object type', 'datasource name', 'table name', 
                'remote column name','local column name', 'worksheet name', 'dashboard name', 'property type', 
                'property name', 'property value','proposed value', 'conversion in TS', 
                'supported in TS', 'supported in Migrator', 'exec_error_details'
            ])

        except Exception as e:
            get_logger().error(f'Error in dashboard_property: {str(e)}')
            error_data = {
                'twb file name': twb_name,
                'object type': 'dashboard property',
                'exec_error_details': 'An unexpected error occurred: ' + str(e)
            }
            df_dashboard_property = pd.DataFrame([error_data])
            df_dashboard_property = df_dashboard_property.reindex(columns=[
                'twb file name', 'object type', 'datasource name', 'table name', 
                'remote column name','local column name', 'worksheet name', 'dashboard name', 'property type', 
                'property name', 'property value','proposed value', 'conversion in TS', 
                'supported in TS', 'supported in Migrator', 'exec_error_details'
            ])
        get_logger().info(f'Dashboard parsing completed for {twb_name}')
        return df_dashboard_property
