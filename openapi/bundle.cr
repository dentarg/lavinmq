# Bundles a modular OpenAPI spec into a single YAML/JSON file by inlining
# external $refs (./paths/*.yaml, ./schemas/*.yaml) and rewriting inward refs
# back at the root spec (../openapi.yaml#/...) to local anchors (#/...).
#
# In-file anchors (#/...) inside non-root fragments are inlined in place, since
# their target pointer is only valid within that fragment's source file.
#
# Usage: crystal run openapi/bundle.cr -- <source.yaml> <output.{yaml,json}>

require "yaml"
require "json"

class Bundler
  @cache = {} of String => JSON::Any

  def initialize(root : String)
    @root_path = File.expand_path(root)
  end

  def bundle : JSON::Any
    rewrite(load(@root_path), File.dirname(@root_path))
  end

  private def load(path : String) : JSON::Any
    absolute = File.expand_path(path)
    @cache[absolute] ||= JSON.parse(YAML.parse(File.read(absolute)).to_json)
  end

  private def resolve(doc : JSON::Any, pointer : String) : JSON::Any
    return doc if pointer.empty? || pointer == "/"
    parts = pointer.lchop('/').split('/').map { |p| p.gsub("~1", "/").gsub("~0", "~") }
    parts.reduce(doc) do |acc, part|
      case raw = acc.raw
      when Hash  then acc[part]
      when Array then acc[part.to_i]
      else            raise "cannot descend into #{raw.class} with #{part.inspect}"
      end
    end
  end

  private def rewrite(value : JSON::Any, base_dir : String, file : String = @root_path) : JSON::Any
    case raw = value.raw
    when Hash(String, JSON::Any)
      if raw.size == 1 && (ref = raw["$ref"]?)
        return inline(ref.as_s, base_dir, file)
      end
      new_hash = {} of String => JSON::Any
      raw.each { |k, v| new_hash[k] = rewrite(v, base_dir, file) }
      JSON::Any.new(new_hash)
    when Array(JSON::Any)
      JSON::Any.new(raw.map { |v| rewrite(v, base_dir, file).as(JSON::Any) })
    else
      value
    end
  end

  private def inline(ref : String, base_dir : String, file : String) : JSON::Any
    if ref.starts_with?('#')
      # Local anchor inside the root spec — keep as-is.
      return JSON::Any.new({"$ref" => JSON::Any.new(ref)}) if file == @root_path
      # Local anchor inside a fragment file — inline its target so the merged
      # spec doesn't end up with dangling pointers (the pointer is only valid
      # within `file`, not the bundled root).
      target = resolve(load(file), ref.lchop('#'))
      return rewrite(target, File.dirname(file), file)
    end

    file_part, _, pointer = ref.partition('#')
    target_path = File.expand_path(file_part, base_dir)

    # Refs pointing back at the root spec become local anchors.
    if target_path == @root_path
      return JSON::Any.new({"$ref" => JSON::Any.new("#" + pointer)})
    end

    target = resolve(load(target_path), pointer)
    rewrite(target, File.dirname(target_path), target_path)
  end
end

source = ARGV[0]? || abort "usage: bundle.cr <source.yaml> <output.{yaml,json}>"
output = ARGV[1]? || abort "usage: bundle.cr <source.yaml> <output.{yaml,json}>"

bundled = Bundler.new(source).bundle
serialized = output.ends_with?(".json") ? bundled.to_json : bundled.to_yaml
File.write(output, serialized)
