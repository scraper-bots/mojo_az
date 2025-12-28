#!/usr/bin/env python3
"""
Business Analytics Dashboard Generator for Mojo.az User Data
Generates business-focused visualizations to support strategic decision-making
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import re
import warnings
warnings.filterwarnings('ignore')

# Set professional style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# Azerbaijani month mapping
AZ_MONTHS = {
    'yanvar': 1, 'fevral': 2, 'mart': 3, 'aprel': 4, 'may': 5, 'iyun': 6,
    'iyul': 7, 'avqust': 8, 'sentyabr': 9, 'oktyabr': 10, 'noyabr': 11, 'dekabr': 12
}

def parse_azerbaijani_date(date_str, scrape_date):
    """Parse Azerbaijani date strings to datetime objects"""
    if pd.isna(date_str):
        return None

    date_str = str(date_str).strip()

    # Handle "Bugün" (Today)
    if date_str.startswith('Bugün'):
        return scrape_date.replace(hour=0, minute=0, second=0, microsecond=0)

    # Handle "Dünən" (Yesterday)
    if date_str.startswith('Dünən'):
        return scrape_date.replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=1)

    # Handle regular dates: "15 dekabr 2019"
    pattern = r'(\d+)\s+(\w+)\s+(\d{4})'
    match = re.search(pattern, date_str)
    if match:
        day, month_name, year = match.groups()
        month = AZ_MONTHS.get(month_name.lower())
        if month:
            try:
                return datetime(int(year), month, int(day))
            except:
                return None

    return None

def load_and_prepare_data():
    """Load and prepare user data for analysis"""
    print("Loading dataset...")
    df = pd.read_csv('mojo_users.csv')

    print(f"Dataset size: {len(df):,} users")

    # Parse scraped_at as reference date
    df['scraped_at'] = pd.to_datetime(df['scraped_at'])

    # Parse dates
    print("Processing dates...")
    df['registration_date_parsed'] = df.apply(
        lambda row: parse_azerbaijani_date(row['registration_date'], row['scraped_at']),
        axis=1
    )
    df['last_seen_date_parsed'] = df.apply(
        lambda row: parse_azerbaijani_date(row['last_seen_date'], row['scraped_at']),
        axis=1
    )

    # Remove records with invalid dates
    df = df.dropna(subset=['registration_date_parsed'])

    # Extract temporal features
    df['registration_year'] = df['registration_date_parsed'].dt.year
    df['registration_month'] = df['registration_date_parsed'].dt.to_period('M')
    df['registration_quarter'] = df['registration_date_parsed'].dt.to_period('Q')

    # Calculate user lifecycle metrics
    df['days_since_registration'] = (df['scraped_at'] - df['registration_date_parsed']).dt.days
    df['days_since_last_seen'] = (df['scraped_at'] - df['last_seen_date_parsed']).dt.days

    # User activity segmentation
    df['is_active_user'] = df['days_since_last_seen'] <= 30
    df['has_listings'] = df['listing_count'] > 0

    # User engagement tiers
    df['user_segment'] = pd.cut(
        df['days_since_last_seen'],
        bins=[-1, 7, 30, 90, 365, float('inf')],
        labels=['Highly Active (Last 7 days)', 'Active (Last 30 days)',
                'Moderate (Last 90 days)', 'Low Activity (Last Year)', 'Inactive']
    )

    print(f"Processed {len(df):,} valid user records\n")
    return df

def generate_user_growth_chart(df):
    """Chart 1: User Growth Over Time"""
    print("Generating Chart 1: User Growth Trends...")

    # Monthly registrations
    monthly_reg = df.groupby('registration_month').size().reset_index(name='new_users')
    monthly_reg['cumulative_users'] = monthly_reg['new_users'].cumsum()
    monthly_reg['registration_month'] = monthly_reg['registration_month'].astype(str)

    fig, ax = plt.subplots(figsize=(14, 7))

    x = range(len(monthly_reg))
    ax.bar(x, monthly_reg['new_users'], alpha=0.7, color='#3498db', label='New Users per Month')
    ax2 = ax.twinx()
    ax2.plot(x, monthly_reg['cumulative_users'], color='#e74c3c', linewidth=2.5,
             marker='o', markersize=4, label='Total Users (Cumulative)')

    ax.set_xlabel('Month', fontsize=12, fontweight='bold')
    ax.set_ylabel('New User Registrations', fontsize=12, fontweight='bold', color='#3498db')
    ax2.set_ylabel('Total User Base', fontsize=12, fontweight='bold', color='#e74c3c')
    ax.set_title('Platform User Growth: New Registrations vs Total User Base',
                 fontsize=14, fontweight='bold', pad=20)

    # Set x-axis labels (show every 3rd month to avoid crowding)
    ax.set_xticks(range(0, len(monthly_reg), 3))
    ax.set_xticklabels(monthly_reg['registration_month'].iloc[::3], rotation=45, ha='right')

    ax.legend(loc='upper left', fontsize=10)
    ax2.legend(loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('charts/1_user_growth_trends.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  ✓ Saved: charts/1_user_growth_trends.png")

def generate_quarterly_growth_chart(df):
    """Chart 2: Quarterly Growth Comparison"""
    print("Generating Chart 2: Quarterly Performance...")

    quarterly = df.groupby('registration_quarter').size().reset_index(name='users')
    quarterly['quarter'] = quarterly['registration_quarter'].astype(str)
    quarterly['year'] = quarterly['registration_quarter'].apply(lambda x: str(x).split('Q')[0])
    quarterly['q'] = quarterly['registration_quarter'].apply(lambda x: 'Q' + str(x).split('Q')[1])

    fig, ax = plt.subplots(figsize=(14, 7))

    colors = ['#1abc9c' if i % 2 == 0 else '#16a085' for i in range(len(quarterly))]
    bars = ax.bar(range(len(quarterly)), quarterly['users'], color=colors, alpha=0.8)

    # Highlight peak quarter
    max_idx = quarterly['users'].idxmax()
    bars[max_idx].set_color('#e74c3c')
    bars[max_idx].set_alpha(1.0)

    ax.set_xlabel('Quarter', fontsize=12, fontweight='bold')
    ax.set_ylabel('New User Registrations', fontsize=12, fontweight='bold')
    ax.set_title('Quarterly User Acquisition Performance', fontsize=14, fontweight='bold', pad=20)

    ax.set_xticks(range(len(quarterly)))
    ax.set_xticklabels(quarterly['quarter'], rotation=45, ha='right')

    # Add value labels on bars
    for i, (idx, row) in enumerate(quarterly.iterrows()):
        ax.text(i, row['users'] + 50, f"{row['users']:,}", ha='center', va='bottom', fontsize=9)

    ax.grid(True, axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig('charts/2_quarterly_performance.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  ✓ Saved: charts/2_quarterly_performance.png")

def generate_user_engagement_chart(df):
    """Chart 3: User Engagement Distribution"""
    print("Generating Chart 3: User Engagement Analysis...")

    segment_counts = df['user_segment'].value_counts().sort_index()

    fig, ax = plt.subplots(figsize=(12, 7))

    colors = ['#27ae60', '#2ecc71', '#f39c12', '#e67e22', '#c0392b']
    bars = ax.barh(range(len(segment_counts)), segment_counts.values, color=colors, alpha=0.85)

    ax.set_yticks(range(len(segment_counts)))
    ax.set_yticklabels(segment_counts.index, fontsize=11)
    ax.set_xlabel('Number of Users', fontsize=12, fontweight='bold')
    ax.set_title('User Engagement Distribution by Activity Level', fontsize=14, fontweight='bold', pad=20)

    # Add percentage labels
    total = segment_counts.sum()
    for i, (count, bar) in enumerate(zip(segment_counts.values, bars)):
        percentage = (count / total) * 100
        ax.text(count + 100, i, f'{count:,} ({percentage:.1f}%)',
                va='center', fontsize=10, fontweight='bold')

    ax.grid(True, axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig('charts/3_user_engagement_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  ✓ Saved: charts/3_user_engagement_distribution.png")

def generate_listing_activity_chart(df):
    """Chart 4: User Listing Activity Analysis"""
    print("Generating Chart 4: Listing Activity Patterns...")

    # Categorize by listing count
    df['listing_category'] = pd.cut(
        df['listing_count'],
        bins=[-0.1, 0, 1, 5, 10, 50, float('inf')],
        labels=['No Listings', '1 Listing', '2-5 Listings',
                '6-10 Listings', '11-50 Listings', '50+ Listings']
    )

    listing_dist = df['listing_category'].value_counts().sort_index()

    fig, ax = plt.subplots(figsize=(12, 7))

    colors = ['#95a5a6', '#3498db', '#2ecc71', '#f39c12', '#e67e22', '#9b59b6']
    bars = ax.bar(range(len(listing_dist)), listing_dist.values, color=colors, alpha=0.85)

    ax.set_xticks(range(len(listing_dist)))
    ax.set_xticklabels(listing_dist.index, rotation=30, ha='right')
    ax.set_ylabel('Number of Users', fontsize=12, fontweight='bold')
    ax.set_title('User Distribution by Listing Activity', fontsize=14, fontweight='bold', pad=20)

    # Add value labels
    total = listing_dist.sum()
    for i, (count, bar) in enumerate(zip(listing_dist.values, bars)):
        percentage = (count / total) * 100
        ax.text(i, count + 50, f'{count:,}\n({percentage:.1f}%)',
                ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.grid(True, axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig('charts/4_listing_activity_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  ✓ Saved: charts/4_listing_activity_distribution.png")

def generate_engagement_vs_listings_chart(df):
    """Chart 5: Relationship Between Engagement and Listings"""
    print("Generating Chart 5: Engagement vs Listing Analysis...")

    # Create cross-tabulation
    engagement_listing = pd.crosstab(
        df['user_segment'],
        df['has_listings'],
        normalize='index'
    ) * 100

    engagement_listing.columns = ['No Listings', 'Has Listings']

    fig, ax = plt.subplots(figsize=(12, 7))

    engagement_listing.plot(kind='barh', stacked=True, ax=ax,
                           color=['#e74c3c', '#2ecc71'], alpha=0.85)

    ax.set_xlabel('Percentage of Users', fontsize=12, fontweight='bold')
    ax.set_ylabel('User Activity Level', fontsize=12, fontweight='bold')
    ax.set_title('Listing Ownership Rate by User Engagement Level',
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(title='Listing Status', loc='lower right', fontsize=10)

    # Add percentage labels
    for i, segment in enumerate(engagement_listing.index):
        no_listing_pct = engagement_listing.loc[segment, 'No Listings']
        has_listing_pct = engagement_listing.loc[segment, 'Has Listings']

        if no_listing_pct > 5:
            ax.text(no_listing_pct/2, i, f'{no_listing_pct:.1f}%',
                   ha='center', va='center', fontsize=10, color='white', fontweight='bold')
        if has_listing_pct > 5:
            ax.text(no_listing_pct + has_listing_pct/2, i, f'{has_listing_pct:.1f}%',
                   ha='center', va='center', fontsize=10, color='white', fontweight='bold')

    ax.grid(True, axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig('charts/5_engagement_vs_listings.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  ✓ Saved: charts/5_engagement_vs_listings.png")

def generate_retention_cohort_chart(df):
    """Chart 6: User Retention Analysis by Registration Year"""
    print("Generating Chart 6: Retention Analysis...")

    retention = df.groupby('registration_year').agg({
        'user_id': 'count',
        'is_active_user': 'sum'
    }).reset_index()

    retention.columns = ['Year', 'Total Users', 'Active Users']
    retention['Inactive Users'] = retention['Total Users'] - retention['Active Users']
    retention['Retention Rate (%)'] = (retention['Active Users'] / retention['Total Users'] * 100).round(1)

    fig, ax = plt.subplots(figsize=(14, 7))

    x = range(len(retention))
    width = 0.35

    bars1 = ax.bar([i - width/2 for i in x], retention['Active Users'],
                   width, label='Active Users', color='#27ae60', alpha=0.85)
    bars2 = ax.bar([i + width/2 for i in x], retention['Inactive Users'],
                   width, label='Inactive Users', color='#e74c3c', alpha=0.85)

    ax.set_xlabel('Registration Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Users', fontsize=12, fontweight='bold')
    ax.set_title('User Retention by Registration Cohort', fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(retention['Year'].astype(int))
    ax.legend(fontsize=10)

    # Add retention rate line
    ax2 = ax.twinx()
    line = ax2.plot(x, retention['Retention Rate (%)'], color='#3498db',
                    linewidth=2.5, marker='o', markersize=8, label='Retention Rate (%)')
    ax2.set_ylabel('Retention Rate (%)', fontsize=12, fontweight='bold', color='#3498db')
    ax2.legend(loc='upper right', fontsize=10)

    # Add value labels
    for i, row in retention.iterrows():
        ax.text(i, row['Total Users'] + 50, f"{row['Retention Rate (%)']}%",
                ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.grid(True, axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig('charts/6_retention_by_cohort.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  ✓ Saved: charts/6_retention_by_cohort.png")

def generate_power_users_chart(df):
    """Chart 7: Power User Analysis"""
    print("Generating Chart 7: Power User Analysis...")

    # Top listing creators
    power_users = df[df['listing_count'] > 0].nlargest(20, 'listing_count')

    fig, ax = plt.subplots(figsize=(14, 8))

    colors = ['#e74c3c' if i < 5 else '#3498db' for i in range(len(power_users))]
    bars = ax.barh(range(len(power_users)), power_users['listing_count'].values,
                   color=colors, alpha=0.85)

    # Create labels (user_id to maintain privacy)
    labels = [f"User {row['user_id']}" for _, row in power_users.iterrows()]

    ax.set_yticks(range(len(power_users)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel('Number of Listings', fontsize=12, fontweight='bold')
    ax.set_title('Top 20 Content Creators by Listing Volume', fontsize=14, fontweight='bold', pad=20)

    # Add value labels
    for i, count in enumerate(power_users['listing_count'].values):
        ax.text(count + 1, i, f'{count:,}', va='center', fontsize=9)

    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#e74c3c', alpha=0.85, label='Top 5 Power Users'),
        Patch(facecolor='#3498db', alpha=0.85, label='Top 6-20 Users')
    ]
    ax.legend(handles=legend_elements, loc='lower right', fontsize=10)

    ax.grid(True, axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig('charts/7_power_users_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  ✓ Saved: charts/7_power_users_analysis.png")

def generate_activity_timeline_chart(df):
    """Chart 8: Monthly Active Users Timeline"""
    print("Generating Chart 8: Active User Timeline...")

    # Calculate monthly metrics
    monthly_data = []

    for month in df['registration_month'].unique():
        month_date = pd.Period(month).to_timestamp()

        # Users registered by this month
        registered = df[df['registration_date_parsed'] <= month_date]

        # Active users in this month (last seen within 30 days of month end)
        month_end = month_date + pd.offsets.MonthEnd(0)
        active = registered[
            (registered['last_seen_date_parsed'] >= month_end - pd.Timedelta(days=30)) &
            (registered['last_seen_date_parsed'] <= month_end)
        ]

        monthly_data.append({
            'month': str(month),
            'total_users': len(registered),
            'active_users': len(active)
        })

    timeline_df = pd.DataFrame(monthly_data)

    fig, ax = plt.subplots(figsize=(14, 7))

    x = range(len(timeline_df))
    ax.fill_between(x, timeline_df['active_users'], alpha=0.3, color='#2ecc71', label='Active Users')
    ax.plot(x, timeline_df['active_users'], color='#27ae60', linewidth=2.5, marker='o', markersize=4)
    ax.plot(x, timeline_df['total_users'], color='#3498db', linewidth=2,
            linestyle='--', label='Total Registered Users', alpha=0.7)

    ax.set_xlabel('Month', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Users', fontsize=12, fontweight='bold')
    ax.set_title('Platform Activity Timeline: Active Users vs Total User Base',
                 fontsize=14, fontweight='bold', pad=20)

    ax.set_xticks(range(0, len(timeline_df), 3))
    ax.set_xticklabels(timeline_df['month'].iloc[::3], rotation=45, ha='right')

    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('charts/8_activity_timeline.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("  ✓ Saved: charts/8_activity_timeline.png")

def generate_summary_statistics(df):
    """Generate key business metrics summary"""
    print("\n" + "="*60)
    print("KEY BUSINESS METRICS SUMMARY")
    print("="*60)

    total_users = len(df)
    active_users = df['is_active_user'].sum()
    users_with_listings = df['has_listings'].sum()
    total_listings = df['listing_count'].sum()

    print(f"\n📊 PLATFORM OVERVIEW")
    print(f"   Total Registered Users: {total_users:,}")
    print(f"   Active Users (Last 30 days): {active_users:,} ({active_users/total_users*100:.1f}%)")
    print(f"   Users with Listings: {users_with_listings:,} ({users_with_listings/total_users*100:.1f}%)")
    print(f"   Total Listings Created: {total_listings:,}")

    print(f"\n📈 GROWTH METRICS")
    yearly_growth = df.groupby('registration_year').size()
    print(f"   Peak Registration Year: {yearly_growth.idxmax()} ({yearly_growth.max():,} users)")

    recent_registrations = df[df['days_since_registration'] <= 30]
    print(f"   New Users (Last 30 days): {len(recent_registrations):,}")

    print(f"\n👥 USER ENGAGEMENT")
    engagement_dist = df['user_segment'].value_counts()
    print(f"   Highly Active Users: {engagement_dist.get('Highly Active (Last 7 days)', 0):,}")
    print(f"   Inactive Users: {engagement_dist.get('Inactive', 0):,}")

    print(f"\n⭐ CONTENT CREATION")
    avg_listings = df[df['listing_count'] > 0]['listing_count'].mean()
    print(f"   Average Listings per Active Creator: {avg_listings:.1f}")
    print(f"   Top Creator Listings: {df['listing_count'].max():,}")

    retention_rate = (active_users / total_users) * 100
    print(f"\n🎯 RETENTION & HEALTH")
    print(f"   Overall Retention Rate: {retention_rate:.1f}%")

    print("\n" + "="*60 + "\n")

def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("MOJO.AZ BUSINESS ANALYTICS DASHBOARD")
    print("="*60 + "\n")

    # Load data
    df = load_and_prepare_data()

    # Generate all charts
    print("Generating business insight charts...\n")
    generate_user_growth_chart(df)
    generate_quarterly_growth_chart(df)
    generate_user_engagement_chart(df)
    generate_listing_activity_chart(df)
    generate_engagement_vs_listings_chart(df)
    generate_retention_cohort_chart(df)
    generate_power_users_chart(df)
    generate_activity_timeline_chart(df)

    # Print summary statistics
    generate_summary_statistics(df)

    print("✅ All charts generated successfully in ./charts/ directory")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
