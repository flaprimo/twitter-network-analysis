from datasources.tw import Tw
tw = Tw()

profile1 = tw.tw_static_scraper.get_profile('pmissier')

# normalized follower ratio from https://doi.org/10.1016/j.ipm.2016.04.003
follower_rank1 = profile1['followers'] / (profile1['followers'] + profile1['following'])

print(f'profile: {profile1}')
print(f'follower_rank: {follower_rank1}')
