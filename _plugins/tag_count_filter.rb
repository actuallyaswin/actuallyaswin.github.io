module Jekyll
  module TagCountFilter
    # Given site.tags (hash of tag name => array of posts) and the fixed
    # tag universe from _data/tags.yml, returns an array of {name:, count:}
    # hashes sorted by count descending, ties broken alphabetically.
    # Tags with zero posts are excluded.
    def tags_by_count(site_tags, all_tag_names)
      all_tag_names
        .map { |name| { "name" => name, "count" => (site_tags[name] || []).size } }
        .select { |entry| entry["count"] > 0 }
        .sort_by { |entry| [-entry["count"], entry["name"]] }
    end
  end
end

Liquid::Template.register_filter(Jekyll::TagCountFilter)
