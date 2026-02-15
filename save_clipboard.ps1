$plain = Get-Clipboard -TextFormatType UnicodeText -Raw
$html  = Get-Clipboard -TextFormatType Html -Raw

if ([string]::IsNullOrWhiteSpace($html)) { Set-Clipboard -Value $plain; exit }

# FIX: Get-Clipboard(Html) can return CF_HTML with incorrect decoding; CF_HTML is UTF-8
# (recode: bytes(Default/ANSI) -> string(UTF8))
$html = [Text.Encoding]::UTF8.GetString([Text.Encoding]::Default.GetBytes($html))

# Extract the selection fragment (CF_HTML)
$start = $html.IndexOf("<!--StartFragment-->")
$end   = $html.IndexOf("<!--EndFragment-->")
if ($start -ge 0 -and $end -gt $start) {
  $frag = $html.Substring($start + "<!--StartFragment-->".Length, $end - ($start + "<!--StartFragment-->".Length))
} else {
  $frag = $html
}

# Save HTML dump for debugging
$scriptDir_early = Split-Path -Parent $MyInvocation.MyCommand.Path
$html | Out-File -FilePath (Join-Path $scriptDir_early "clipboard_html_dump.txt") -Encoding utf8 -NoNewline

# Replace links: <a href="URL">TEXT</a> => TEXT [URL]
$reA = [regex]::new('<a\b[^>]*href\s*=\s*("(?<u>[^"]+)|''(?<u>[^'']+)''|(?<u>[^\s>]+))[^>]*>(?<t>.*?)</a>', 'IgnoreCase,Singleline')
$out = $reA.Replace($frag, {
  param($m)
  $u = $m.Groups['u'].Value.Trim()
  $t = [regex]::Replace($m.Groups['t'].Value, '<[^>]+>', '')  # remove tags inside anchor text
  $t = $t -replace '\s+', ' '
  if ([string]::IsNullOrWhiteSpace($t)) { "[$u]" } else { "$t [$u]" }
})

# Convert <time datetime="2026-02-14T20:02:53.534Z">21:02</time> to [YYYY-MM-DDTHH:MM]
# Must happen BEFORE stripping HTML tags. Converts UTC to local timezone.
$out = [regex]::Replace($out, '<time[^>]*datetime="([^"]+)"[^>]*>[^<]*</time>', {
  param($m)
  $utcStr = $m.Groups[1].Value
  $utcDt = [DateTimeOffset]::Parse($utcStr)
  $localDt = $utcDt.ToLocalTime()
  "[" + $localDt.ToString("yyyy-MM-ddTHH:mm") + "]"
})

# Extract date from first <time> tag for filename (already converted above, so parse from text)
$firstTimeMatch = [regex]::Match($out, '\[(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}\]')
if ($firstTimeMatch.Success) {
  $userDate = $firstTimeMatch.Groups[1].Value + $firstTimeMatch.Groups[2].Value + $firstTimeMatch.Groups[3].Value
} else {
  # Fallback: extract date from Discord snowflake message ID
  $snowflakeMatch = [regex]::Match($frag, 'chat-messages-\d+-(\d{17,20})')
  if ($snowflakeMatch.Success) {
    $snowflakeId = [long]$snowflakeMatch.Groups[1].Value
    $discordEpoch = 1420070400000
    $unixMs = ($snowflakeId -shr 22) + $discordEpoch
    $baseDt = [DateTimeOffset]::FromUnixTimeMilliseconds($unixMs).ToLocalTime()
    $userDate = $baseDt.ToString("yyyyMMdd")
    $baseDate = $baseDt.ToString("yyyy-MM-dd")
    Write-Host "Date from Discord message ID: $baseDate" -ForegroundColor Cyan

    # Inject full datetime into time-only markers [HH:MM] -> [YYYY-MM-DDTHH:MM]
    $out = [regex]::Replace($out, '\[(\d{2}:\d{2})\]', "[$baseDate" + 'T$1]')
  } else {
    # Last resort: ask user
    $defaultDate = Get-Date -Format "yyyyMMdd"
    Write-Host "Could not detect date. Enter date (YYYYMMDD) [$defaultDate]:" -ForegroundColor Yellow
    $input = Read-Host
    if ([string]::IsNullOrWhiteSpace($input)) { $userDate = $defaultDate }
    else { $userDate = $input }
  }
}

# HTML decode + minimal block-level tag replacements to newlines, strip other tags
Add-Type -AssemblyName System.Web
$out = [System.Web.HttpUtility]::HtmlDecode($out)  # decodes &amp;, etc.
$out = [regex]::Replace($out, '(?i)<\s*br\s*/?\s*>', "`r`n")
$out = [regex]::Replace($out, '(?i)</\s*(div|p|li|tr|h[1-6])\s*>', "`r`n")
$out = [regex]::Replace($out, '<[^>]+>', '')

# Whitespace cleanup
$out = $out -replace "\r?\n[ \t]+", "`r`n"     # trim indentation after newlines
$out = $out -replace "[ \t]{2,}", " "          # collapse multiple spaces to 1 (without touching newlines)
$out = $out -replace "(\r?\n){3,}", "`r`n`r`n" # max 1 blank line
$out = $out.Trim()

# Create input/ directory if it doesn't exist
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$inputDir = Join-Path $scriptDir "input"
if (-not (Test-Path $inputDir)) {
  New-Item -ItemType Directory -Path $inputDir | Out-Null
}

# Generate filename with timestamp for uniqueness
$timeStamp = Get-Date -Format "HHmmss"
$filename = "${userDate}_${timeStamp}_discord.txt"
$filepath = Join-Path $inputDir $filename

# Save to file
$out | Out-File -FilePath $filepath -Encoding utf8 -NoNewline

Write-Host "Saved to: $filepath" -ForegroundColor Green
