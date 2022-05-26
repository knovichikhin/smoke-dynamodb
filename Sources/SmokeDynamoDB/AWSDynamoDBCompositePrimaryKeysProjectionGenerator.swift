// Copyright 2018-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// A copy of the License is located at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//  AWSDynamoDBCompositePrimaryKeysProjectionGenerator.swift
//  SmokeDynamoDB
//

import AWSClientRuntime
import Logging

public class AWSDynamoDBCompositePrimaryKeysProjectionGenerator {
    private let dynamodbGenerator: ClientGenerator
    internal let targetTableName: String
    
    private enum ClientGenerator {
        case fromConfig(AWSClientRuntime.AWSClientConfiguration)
        case fromRegion(String)
        case asDefault
    }
    
    public init(config: AWSClientRuntime.AWSClientConfiguration,
                tableName: String) {
        self.dynamodbGenerator = .fromConfig(config)
        self.targetTableName = tableName
    }
    
    public init(region: String,
                tableName: String) {
        self.dynamodbGenerator = .fromRegion(region)
        self.targetTableName = tableName
    }
    
    public init(tableName: String) async throws {
        self.dynamodbGenerator = .asDefault
        self.targetTableName = tableName
    }

    public func with(logger: Logging.Logger) async throws
    -> AWSDynamoDBCompositePrimaryKeysProjection {
        switch self.dynamodbGenerator {
        case .fromConfig(let config):
            return AWSDynamoDBCompositePrimaryKeysProjection(config: config, tableName: self.targetTableName, logger: logger)
        case .fromRegion(let region):
            return try AWSDynamoDBCompositePrimaryKeysProjection(region: region, tableName: self.targetTableName, logger: logger)
        case .asDefault:
            return try await AWSDynamoDBCompositePrimaryKeysProjection(tableName: self.targetTableName, logger: logger)
        }
    }
}
