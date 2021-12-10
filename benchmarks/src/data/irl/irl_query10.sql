SELECT 
  hashtags.*, invites_hashtags.invite_id, invites_hashtags.hashtag_id, invites_hashtags.created_at, invites_hashtags.updated_at
  FROM hashtags INNER JOIN invites_hashtags ON hashtags.id = invites_hashtags.hashtag_id
  WHERE invites_hashtags.invite_id in (?, ?, ?)
