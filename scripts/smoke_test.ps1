# scripts/smoke_test.ps1

param()

Write-Host "Running Chrysalis Phase10 smoke-test..."



# helper

function Exec-Curl($url, $bodyPath) {

    $body = Get-Content -Raw $bodyPath

    $res = Invoke-RestMethod -Uri $url -Method Post -Body $body -ContentType 'application/json'

    return $res

}



# 1 - health

$h = curl.exe -s http://127.0.0.1:8000/health | Select-String -Pattern '"status":"ok"' -Quiet

if (-not $h) { Write-Host "Health failed"; exit 2 } else { Write-Host "Health OK" }



# 2 - post test batches (fixtures must exist)

$fixtures = @(

  ".\fixtures\test_batch.json",

  ".\fixtures\batch_B.json",

  ".\fixtures\batch_D_missing_required.json",

  ".\fixtures\batch_E_bad_type.json"

)

foreach ($f in $fixtures) {

    if (-not (Test-Path $f)) { Write-Host "Missing fixture $f"; exit 3 }

    Write-Host "Posting $f ..."

    $resp = Exec-Curl "http://127.0.0.1:8000/ingest" $f

    Write-Host " -> job: $($resp.job_id) status: $($resp.status)"

}



# 3 - post mixed format files using CLI

Write-Host "Posting mixed format files..."

$mixedFiles = @(

  ".\fixtures\mixed\sample.csv",

  ".\fixtures\mixed\sample.html",

  ".\fixtures\mixed\sample.txt"

)

foreach ($f in $mixedFiles) {

    if (-not (Test-Path $f)) { Write-Host "Missing fixture $f"; continue }

    Write-Host "Converting $f to job payload..."

    try {

        $jobJson = docker exec chrysalis_worker python /app/cli/ingest_file.py /app/$($f.Replace('\', '/').Replace('.\', '')) 2>&1

        if ($LASTEXITCODE -eq 0) {

            $jobObj = $jobJson | ConvertFrom-Json

            $body = $jobJson

            $resp = Invoke-RestMethod -Uri "http://127.0.0.1:8000/ingest" -Method Post -Body $body -ContentType 'application/json'

            Write-Host " -> job: $($resp.job_id) status: $($resp.status)"

        } else {

            Write-Host " -> Failed to convert $f"

        }

    } catch {

        Write-Host " -> Error processing $f : $_"

    }

}



Start-Sleep -Seconds 3



# 4 - check DLQ length

$dlqLen = docker exec chrysalis_redis redis-cli LLEN chrysalis:dlq

Write-Host "DLQ length (raw): $dlqLen"



# 5 - query mongo for raw_data count and schema registry count

$raw_count = docker exec chrysalis_mongo mongosh chrysalis --quiet --eval "db.raw_data.find().count()"

$schema_count = docker exec chrysalis_mongo mongosh chrysalis --quiet --eval "db.schema_registry.find().count()"

Write-Host "raw_data count: $raw_count"

Write-Host "schema_registry count: $schema_count"



Write-Host "smoke-test finished. Inspect outputs above for pass/fail."
