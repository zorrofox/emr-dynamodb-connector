/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */
 
package org.apache.hadoop.dynamodb.read;

import com.amazonaws.services.dynamodbv2.model.TableDescription;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.filter.AbstractDynamoDBFilter;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterPushdown;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.dynamodb.util.SerializeUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Created by Greg on 2021. 10. 7
 */
public class DynamoDBQueryInputFormat extends 
    AbstractDynamoDBInputFormat<Text, DynamoDBItemWritable> {
  private static final Log log = LogFactory.getLog(DynamoDBQueryInputFormat.class);

  @Override
  public RecordReader<Text, DynamoDBItemWritable> getRecordReader(InputSplit split, JobConf conf,
      Reporter reporter) throws IOException {
    reporter.progress();
    
    DynamoDBQueryFilter queryFilter = getQueryFilter(conf);
    DynamoDBSplit bbSplit = (DynamoDBSplit) split;
    bbSplit.setDynamoDBFilterPushdown(queryFilter);
    DynamoDBRecordReaderContext context = buildDynamoDBRecordReaderContext(split, conf, reporter);
    return new DefaultDynamoDBRecordReader(context);
  }
  
  @Override
  protected int getNumSegments(int tableNormalizedReadThroughput, int
      tableNormalizedWriteThroughput, long currentTableSizeBytes, JobConf conf) throws IOException {
    if (isQuery(conf)) {
      log.info("Defaulting to 1 segment because there are key conditions");
      return 1;
    } else {
      return super.getNumSegments(tableNormalizedReadThroughput, tableNormalizedWriteThroughput,
          currentTableSizeBytes, conf);
    }
  }

  @Override
  protected int getNumMappers(int maxClusterMapTasks, int configuredReadThroughput, JobConf conf)
      throws IOException {
    if (isQuery(conf)) {
      log.info("Defaulting to 1 mapper because there are key conditions");
      return 1;
    } else {
      return super.getNumMappers(maxClusterMapTasks, configuredReadThroughput, conf);
    }
  }


  private DynamoDBQueryFilter getQueryFilter(JobConf conf) {

    try {
      ArrayList<AbstractDynamoDBFilter> filterList = 
          (ArrayList) SerializeUtil
            .fromString(conf.get(DynamoDBConstants.DYNAMODB_MULTIPLE_QUERY));
      if (filterList.isEmpty()) {
        throw new IllegalArgumentException(this.getClass().getCanonicalName()
                 + " must need to 1 more filters.");
      }
      
      log.info("Filter list size: "  + filterList.size());
      DynamoDBClient client = new DynamoDBClient(conf);
      DynamoDBFilterPushdown pushdown = new DynamoDBFilterPushdown();
      ArrayList<String> projections = 
          new ArrayList<>(Arrays
            .asList(conf
               .get(DynamoDBConstants.DYNAMODB_FILTER_PROJECTION).split(",")));
      
      TableDescription tableDescription =
          client.describeTable(conf.get(DynamoDBConstants.TABLE_NAME));
      DynamoDBQueryFilter queryFilter = pushdown.predicateToDynamoDBFilter(
          tableDescription.getKeySchema(),
          tableDescription.getLocalSecondaryIndexes(),
          tableDescription.getGlobalSecondaryIndexes(),
          projections,
          filterList);
      return queryFilter; 
 
    } catch (ClassNotFoundException | IOException e) {
      log.error(e);
      throw new IllegalArgumentException(this.getClass().getCanonicalName()
                 + " can't load multi table filters.");
    }
  }
  
  private boolean isQuery(JobConf conf) throws IOException {
    DynamoDBQueryFilter filter = getQueryFilter(conf);

    return filter.getKeyConditions().size() >= 1;
  }
  
}