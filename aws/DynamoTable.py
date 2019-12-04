import boto3

class DynamoTable:

    def __init__(self, table):
        """
        Initialize Table
        """
        self.resource = boto3.resource('dynamodb')
        self.table = resource.Table(table)

    def read_table_item(self, table_name, pk_name, pk_value):
        """
        Return item read by primary key.
        """
        response = self.table.get_item(Key={pk_name: pk_value})
        return response

    def add_item(self, col_dict):
        """
        Add one item (row) to table. col_dict is a dictionary {col_name: value}.
        """
        response = self.table.put_item(Item=col_dict)
        return response

    def delete_item(self, pk_name, pk_value):
        """
        Delete an item (row) in table from its primary key.
        """
        response = self.table.delete_item(Key={pk_name: pk_value})
        return

    def scan_table(self, filter_key=None, filter_value=None):
        """
        Perform a scan operation on table.
        Can specify filter_key (col name) and its value to be filtered.
        """
        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = self.table.scan(FilterExpression=filtering_exp)
        else:
            response = self.table.scan()
        return response


    def query_table(self, filter_key=None, filter_value=None):
        """
        Perform a query operation on the table. 
        Can specify filter_key (col name) and its value to be filtered.
        """
        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = self.table.query(KeyConditionExpression=filtering_exp)
        else:
            response = self.table.query()
        return response

    def scan_table_firstpage(self, filter_key=None, filter_value=None):
        """
        Perform a scan operation on table. Can specify filter_key (col name) and its value to be filtered. This gets only first page of results in pagination. Returns the response.
        """
        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = self.table.scan(FilterExpression=filtering_exp)
        else:
            response = self.table.scan()
        return response

    def scan_table_allpages(self, filter_key=None, filter_value=None):
        """
        Perform a scan operation on table. 
        Can specify filter_key (col name) and its value to be filtered. 
        This gets all pages of results. Returns list of items.
        """
        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = self.table.scan(FilterExpression=filtering_exp)
        else:
            response = self.table.scan()

        items = response['Items']
        while True:
            print len(response['Items'])
            if response.get('LastEvaluatedKey'):
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items += response['Items']
            else:
                break
        return items

    def get_table_metadata(self):
        """
        Get some metadata about chosen table.
        """
        metadata = {
            'num_items': self.table.item_count,
            'primary_key_name': self.table.key_schema[0],
            'status': self.table.table_status,
            'bytes_size': self.table.table_size_bytes,
            'global_secondary_indices': self.table.global_secondary_indexes
        }
        return metadata

    def update_item(self, pk_name, pk_value, col_dict):
        """
        update one item (row) to table. col_dict is a dictionary {col_name: value}.
        """

        update_expression = 'SET {}'.format(','.join(f'#{k}=:{k}' for k in col_dict))
        expression_attribute_values = {f':{k}': v for k, v in col_dict.items()}
        expression_attribute_names = {f'#{k}': k for k in col_dict}

        response = self.table.update_item(
            Key={'{}'.format(pk_name): pk_value},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names,
            ReturnValues='UPDATED_NEW',
        )