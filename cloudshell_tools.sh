export AWS_DEFAULT_REGION=us-east-1
stagestats () {
  local s="$1"
  local e=$(date -u -d "@$(( $(date -u -d "$2" +%s) + 15 ))" +%Y-%m-%dT%H:%M:%S.000Z)
  local vals="{\":s\":{\"S\":\"$s\"},\":e\":{\"S\":\"$e\"}}"
  aws dynamodb scan --table-name AnimalStressData \
    --projection-expression "#ts,processed_at,edge_wait_ms,batch_id,invocation_id,severity,alert_sent" \
    --expression-attribute-names '{"#ts":"timestamp"}' \
    --filter-expression "processed_at BETWEEN :s AND :e" \
    --expression-attribute-values "$vals" --output json > /tmp/bio.json
  aws dynamodb scan --table-name EnvironmentData \
    --projection-expression "batch_id,invocation_id" \
    --filter-expression "processed_at BETWEEN :s AND :e" \
    --expression-attribute-values "$vals" --output json > /tmp/env.json
  jq -n -c --slurpfile b /tmp/bio.json --slurpfile v /tmp/env.json '
    def ms: (.[0:19] + "Z" | fromdateiso8601) * 1000 + (.[20:23] | tonumber);
    def lat: (.processed_at.S | ms) - (.timestamp.S | ms);
    def pct($a; $p): if ($a | length) == 0 then null else $a[(($a | length) * $p | floor)] end;
    ($b[0].Items) as $bio | ($v[0].Items) as $env |
    ([$bio[] | lat] | sort) as $all |
    ([$bio[] | select(.alert_sent.BOOL == true) | lat] | sort) as $alert |
    ([$bio[] | (.edge_wait_ms.N // "0") | tonumber] | sort) as $wait |
    {
      stored: (($bio | length) + ($env | length)),
      bio_rows: ($bio | length),
      env_rows: ($env | length),
      cloud_requests: ([$bio[], $env[] | .batch_id.S] | unique | length),
      prediction_invocations: ([$bio[], $env[] | .invocation_id.S] | unique | length),
      latency_p50: pct($all; 0.5),
      latency_p95: pct($all; 0.95),
      latency_p99: pct($all; 0.99),
      latency_max: ($all | last),
      edge_wait_p50: pct($wait; 0.5),
      edge_wait_p95: pct($wait; 0.95),
      edge_wait_max: ($wait | last),
      stressed_readings: ([$bio[] | select(.severity.S == "medium" or .severity.S == "high")] | length),
      alerts_sent: ($alert | length),
      alert_latency_p50: pct($alert; 0.5),
      alert_latency_p95: pct($alert; 0.95),
      alert_latency_max: ($alert | last)
    }'
}
allstats () {
  tail -n +2 "$1" | tr -d '"\r' | cut -d, -f1,2,5,6,8 | while IFS=, read mode nodes started ended ok; do
    echo "mode=$mode nodes=$nodes accepted=$ok"
    stagestats "$started" "$ended"
  done
}
runtotals () {
  local s=$(date -u -d "@$(( $(date -u -d "$1" +%s) / 60 * 60 ))" +%Y-%m-%dT%H:%M:%SZ)
  local e=$(date -u -d "@$(( ($(date -u -d "$2" +%s) / 60 + 3) * 60 ))" +%Y-%m-%dT%H:%M:%SZ)
  local api=$(aws apigatewayv2 get-apis --query "Items[?Name=='WildlifeStressApi'].ApiId" --output text)
  m () {
    aws cloudwatch get-metric-statistics --namespace "$1" --metric-name "$2" --dimensions "$3" \
      --start-time "$s" --end-time "$e" --period 60 --statistics "$4" --output json |
    jq "[.Datapoints[].$4] | if length == 0 then 0 elif \"$4\" == \"Maximum\" then max else add end"
  }
  echo "Window: $s to $e"
  echo "API Gateway requests:              $(m AWS/ApiGateway Count Name=ApiId,Value=$api Sum)"
  for f in ingestionFunction predictionFunction alertingFunction; do
    echo "$f invocations:  $(m AWS/Lambda Invocations Name=FunctionName,Value=$f Sum)"
    echo "$f duration sum ms: $(m AWS/Lambda Duration Name=FunctionName,Value=$f Sum)"
    echo "$f peak concurrency: $(m AWS/Lambda ConcurrentExecutions Name=FunctionName,Value=$f Maximum)"
    echo "$f errors / throttles: $(m AWS/Lambda Errors Name=FunctionName,Value=$f Sum) / $(m AWS/Lambda Throttles Name=FunctionName,Value=$f Sum)"
  done
  echo "SQS messages sent:                 $(m AWS/SQS NumberOfMessagesSent Name=QueueName,Value=StressReadingsQueue Sum)"
  echo "SQS max messages visible:          $(m AWS/SQS ApproximateNumberOfMessagesVisible Name=QueueName,Value=StressReadingsQueue Maximum)"
  echo "SQS max age of oldest message (s): $(m AWS/SQS ApproximateAgeOfOldestMessage Name=QueueName,Value=StressReadingsQueue Maximum)"
  echo "DLQ max messages visible:          $(m AWS/SQS ApproximateNumberOfMessagesVisible Name=QueueName,Value=StressReadingsDLQ Maximum)"
}
resettables () {
  for t in AnimalStressData EnvironmentData AlertState; do aws dynamodb delete-table --table-name $t >/dev/null; done
  for t in AnimalStressData EnvironmentData AlertState; do aws dynamodb wait table-not-exists --table-name $t; done
  aws dynamodb create-table --table-name AnimalStressData \
    --attribute-definitions AttributeName=animal_id,AttributeType=S AttributeName=timestamp,AttributeType=S \
    --key-schema AttributeName=animal_id,KeyType=HASH AttributeName=timestamp,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST >/dev/null
  aws dynamodb create-table --table-name EnvironmentData \
    --attribute-definitions AttributeName=region,AttributeType=S AttributeName=timestamp_sensor,AttributeType=S \
    --key-schema AttributeName=region,KeyType=HASH AttributeName=timestamp_sensor,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST >/dev/null
  aws dynamodb create-table --table-name AlertState \
    --attribute-definitions AttributeName=animal_id,AttributeType=S \
    --key-schema AttributeName=animal_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
  for t in AnimalStressData EnvironmentData AlertState; do aws dynamodb wait table-exists --table-name $t; echo "$t empty and ready"; done
}
