
    require 'rdf'
    require 'rdf/ntriples'
    require 'data_objects'
    require 'do_sqlite3'
    require 'enumerator'
    
    module RDF
      module DataObjects
        class Repository < ::RDF::Repository
    
          def initialize(options)
            @db = ::DataObjects::Connection.new(options)
            exec('CREATE TABLE IF NOT EXISTS quads (
                  `subject` varchar(255), 
                  `predicate` varchar(255),
                  `object` varchar(255), 
                  `context` varchar(255), 
                  UNIQUE (`subject`, `predicate`, `object`, `context`))')
          end
   
          # @see RDF::Enumerable#each.
          def each(&block)
            if block_given?
              reader = result('select * from quads')
              while reader.next!
                block.call(RDF::Statement.new(
                      :subject   => unserialize(reader.values[0]),
                      :predicate => unserialize(reader.values[1]),
                      :object    => unserialize(reader.values[2]),
                      :context   => unserialize(reader.values[3])))

              end
            else
              ::Enumerable::Enumerator.new(self,:each)
            end
          end
    
          # @see RDF::Mutable#insert_statement
          def insert_statement(statement)
            sql = 'REPLACE INTO `quads` (subject, predicate, object, context) VALUES (?, ?, ?, ?)'
            exec(sql,serialize(statement.subject),serialize(statement.predicate), 
                     serialize(statement.object), serialize(statement.context)) 
          end
    
          # @see RDF::Mutable#delete_statement
          def delete_statement(statement)
            sql = 'DELETE FROM `quads` where (subject = ? AND predicate = ? AND object = ? AND context = ?)'
            exec(sql,serialize(statement.subject),serialize(statement.predicate), 
                     serialize(statement.object), serialize(statement.context)) 
          end
    
    
          ## These are simple helpers to serialize and unserialize component
          # fields.  We use an explicit 'nil' string for null values for clarity in
          # this example; we cannot use NULL, as SQLite considers NULLs as
          # distinct from each other when using the uniqueness constraint we
          # added when we created the table.
          def serialize(value)
            value.nil? ? 'nil' : RDF::NTriples::Writer.serialize(value)
          end
          def unserialize(value)
            value == 'nil' ? nil : RDF::NTriples::Reader.unserialize(value)
          end
    
          ## These are simple helpers for DataObjects
          def exec(sql, *args)
            @db.create_command(sql).execute_non_query(*args)
          end
          def result(sql, *args)
            @db.create_command(sql).execute_reader(*args)
          end
    
          def count
            result = result('select count(*) from quads')
            result.next!
            result.values.first
          end

          def query(pattern, &block)
            case pattern
              when RDF::Statement
                query(pattern.to_hash)
              when Array
                query(RDF::Statement.new(*pattern))
              when Hash
                statements = []
                reader = query_hash(pattern)
                while reader.next!
                  statements << RDF::Statement.new(
                          :subject   => unserialize(reader.values[0]),
                          :predicate => unserialize(reader.values[1]),
                          :object    => unserialize(reader.values[2]),
                          :context   => unserialize(reader.values[3]))
                end
                case block_given?
                  when true
                    statements.each(&block)
                  else
                    statements.extend(RDF::Enumerable, RDF::Queryable)
                end
              else
                super(pattern) 
            end
          end

          def query_hash(hash)
            conditions = []
            params = []
            [:subject, :predicate, :object, :context].each do |resource|
              unless hash[resource].nil?
                conditions << "#{resource.to_s} = ?"
                params     << serialize(hash[resource])
              end
            end
            where = conditions.empty? ? "" : "WHERE "
            where << conditions.join(' AND ')
            result('select * from quads ' + where, *params)
          end


        end
      end
    end
