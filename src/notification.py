import base64
import json
import os
import urllib.error
import urllib.request

import pandas as pd


def build_revenue_alert_summary(result, threshold):
    roller_total = round(_sum_money(result["ROLLER_REVENUE"]), 2)
    snowflake_total = round(_sum_money(result["SNOWFLAKE_REVENUE"]), 2)
    total_variance = round(abs(snowflake_total - roller_total), 2)
    mismatch_count = int((result["MATCH"] == "Mismatch").sum())
    checked_dates = _format_checked_dates(result["DATE"])

    mismatches = (
        result[result["MATCH"] == "Mismatch"]
        .copy()
        .sort_values("VARIANCE", ascending=False)
        .head(10)
    )

    return {
        "roller_total": roller_total,
        "snowflake_total": snowflake_total,
        "total_variance": total_variance,
        "threshold": threshold,
        "checked_dates": checked_dates,
        "mismatch_count": mismatch_count,
        "top_mismatches": mismatches,
        "should_notify": total_variance > threshold,
    }


def notify_if_revenue_discrepancy(
    result,
    output_file,
    webhook_url,
    threshold,
    include_excel_file=True,
):
    summary = build_revenue_alert_summary(result, threshold)

    if not summary["should_notify"]:
        print(
            "No Teams alert needed. Total revenue variance "
            f"${summary['total_variance']:,.2f} is within threshold "
            f"${threshold:,.2f}."
        )
        return summary

    if not webhook_url:
        print("Teams alert skipped because TEAMS_WEBHOOK_URL is not configured.")
        print(_plain_text_alert(summary, output_file))
        return summary

    payload = _teams_payload(summary, output_file, include_excel_file=include_excel_file)

    try:
        status, response_text = _post_teams_webhook(webhook_url, payload)
        if include_excel_file and payload.get("reportAttachment"):
            print("Teams discrepancy alert sent with Excel file payload.")
        else:
            print("Teams discrepancy alert sent.")
        print(f"Teams webhook response status: {status}")
        if response_text:
            print(f"Teams webhook response: {response_text[:500]}")
    except RuntimeError:
        if include_excel_file and payload.get("reportAttachment"):
            print("Teams alert with Excel payload failed. Retrying message-only alert...")
            status, response_text = _post_teams_webhook(webhook_url, _teams_payload(summary, output_file))
            print("Teams discrepancy alert sent without Excel file payload.")
            print(f"Teams webhook response status: {status}")
            if response_text:
                print(f"Teams webhook response: {response_text[:500]}")
        else:
            raise

    return summary


def _sum_money(series):
    return pd.to_numeric(series, errors="coerce").fillna(0).sum()


def _format_checked_dates(series):
    dates = (
        pd.to_datetime(series, errors="coerce")
        .dropna()
        .dt.strftime("%Y-%m-%d")
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    if not dates:
        return "Unknown"

    if len(dates) == 1:
        return dates[0]

    return f"{dates[0]} through {dates[-1]}"


def _post_teams_webhook(webhook_url, payload):
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status >= 400:
                raise RuntimeError(f"Teams webhook returned HTTP {response.status}")
            response_text = response.read().decode("utf-8", errors="replace").strip()
            return response.status, response_text
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to send Teams notification: {exc}") from exc


def _teams_payload(summary, output_file, include_excel_file=False):
    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": "Revenue discrepancy detected",
        "themeColor": "D13438",
        "title": "Revenue discrepancy detected",
        "sections": [
            {
                "facts": [
                    {"name": "Roller total", "value": f"${summary['roller_total']:,.2f}"},
                    {"name": "Snowflake total", "value": f"${summary['snowflake_total']:,.2f}"},
                    {"name": "Total variance", "value": f"${summary['total_variance']:,.2f}"},
                    {"name": "Revenue date(s)", "value": summary["checked_dates"]},
                    {"name": "Mismatched rows", "value": str(summary["mismatch_count"])},
                    {"name": "Report", "value": str(output_file)},
                ],
                "markdown": True,
            },
            {
                "activityTitle": "Largest discrepancies",
                "text": _format_top_mismatches(summary["top_mismatches"]),
                "markdown": True,
            },
        ],
    }

    if include_excel_file:
        attachment = _excel_attachment(output_file)
        if attachment:
            payload["reportAttachment"] = attachment

    return payload


def _excel_attachment(output_file):
    if not output_file or not os.path.exists(output_file):
        print(f"Excel attachment skipped because report was not found: {output_file}")
        return None

    with open(output_file, "rb") as file:
        content_bytes = base64.b64encode(file.read()).decode("ascii")

    return {
        "name": os.path.basename(output_file),
        "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "contentBytes": content_bytes,
    }


def _format_top_mismatches(mismatches):
    if mismatches.empty:
        return "No row-level mismatches were found."

    lines = []
    for row in mismatches.itertuples(index=False):
        date_value = pd.to_datetime(row.DATE).strftime("%Y-%m-%d")
        lines.append(
            f"- {date_value} | {row.VENUE}: "
            f"Roller ${row.ROLLER_REVENUE:,.2f}, "
            f"Snowflake ${row.SNOWFLAKE_REVENUE:,.2f}, "
            f"Variance ${row.VARIANCE:,.2f}"
        )

    return "\n".join(lines)


def _plain_text_alert(summary, output_file):
    return "\n".join(
        [
            "Revenue discrepancy detected",
            f"Roller total: ${summary['roller_total']:,.2f}",
            f"Snowflake total: ${summary['snowflake_total']:,.2f}",
            f"Total variance: ${summary['total_variance']:,.2f}",
            f"Revenue date(s): {summary['checked_dates']}",
            f"Mismatched rows: {summary['mismatch_count']}",
            f"Report: {output_file}",
        ]
    )
