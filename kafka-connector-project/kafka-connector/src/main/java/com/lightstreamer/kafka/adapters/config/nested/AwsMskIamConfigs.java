
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.adapters.config.nested;

import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;

public class AwsMskIamConfigs {

    public static final String CREDENTIAL_PROFILE_NAME = "iam.credential.profile.name";
    public static final String ROLE_ARN = "iam.role.arn";
    public static final String ROLE_SESSION_NAME = "iam.role.session.name";
    public static final String STS_REGION = "iam.sts.region";

    private static ConfigsSpec AWS_MSK_IAM_CONFIG_SPEC;

    static {
        AWS_MSK_IAM_CONFIG_SPEC =
                new ConfigsSpec("awsIam")
                        .add(CREDENTIAL_PROFILE_NAME, false, false, TEXT)
                        .add(ROLE_ARN, false, false, TEXT)
                        .withEnabledChildConfigs(
                                new ConfigsSpec("awsIamRoleArn")
                                        .add(ROLE_SESSION_NAME, false, false, TEXT)
                                        .add(STS_REGION, false, false, TEXT),
                                (map, key) -> map.get(key) != null,
                                ROLE_ARN);
    }

    public static ConfigsSpec spec() {
        return AWS_MSK_IAM_CONFIG_SPEC;
    }

    private AwsMskIamConfigs() {}
}
