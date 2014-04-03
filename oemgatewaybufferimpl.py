"""

  This code is released under the GNU Affero General Public License.
  
  OpenEnergyMonitor project:
  http://openenergymonitor.org

"""

import MySQLdb
import logging

"""class AbstractBuffer

Represents the actual buffer being used.
"""
class AbstractBuffer():
  def storeItem(self,data): pass
  def retrieveItem(self): pass
  def removeLastRetrievedItem(self): pass
  def size(self): pass

"""
This implementation of the AbstractBuffer just uses an in-memory datastructure.
It's basically identical to the previous (inline) buffer.
"""
class InMemoryBuffer(AbstractBuffer):
  maximumEntriesInBuffer = 1000
  
  def __init__(self, name):
    self._data_buffer = []
    
  def isFull(self):
    return self.size() >= InMemoryBuffer.maximumEntriesInBuffer

  def discardOldestItem(self):
    self._data_buffer = self._data_buffer[size - InMemoryBuffer.maximumEntriesInBuffer:]
      
  def discardOldestItemIfFull(self):
    if self.isFull():
      self.discardOldestItem()
        
  def storeItem(self,data):
    self.discardOldestItemIfFull();
            
    self._data_buffer.append (data)
    
  def retrieveItem(self):
    return self._data_buffer[0]
    
  def removeLastRetrievedItem(self):
    del self._data_buffer[0]
    
  def size(self):
    return len(self._data_buffer)
    
    
"""
This implementation of the AbstractBuffer uses MySQL.
"""
class MySQLBuffer(AbstractBuffer):
  
  def openDbConnection(self):
    dbConfig = {
      'host' : 'localhost',
      'user' : 'root',
      'password' : 'raspberry',
      'database' : 'buffer' 
    }
    return MySQLdb.connect(
             dbConfig['host'], 
             dbConfig['user'],
             dbConfig['password'],
             dbConfig['database'] 
           )
  def handleMySQLdbError (self, e):
    try:
      self._log.error ("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
    except IndexError:
      self._log.error ("MySQL Error: %s" % str(e)  )
      
  def __init__(self, name):
    self.bufferName = name
    self._conn = self.openDbConnection();
    self.lastRetrievedItemId = -1
    self._log = logging.getLogger("OemGateway")
  
  def discardOldestItemIfFull(self): pass
        
  def storeItem(self,data):
    self._log.debug("MySQL (%s): storing input from node %s (%s) at %s" % \
                     ( self.bufferName,
                       data[1][0],
                       data[1][1:],
                       data[0]) )
    self.discardOldestItemIfFull();
            
    try:
      dbCursor = self._conn.cursor()
      dbCursor.execute ( "INSERT INTO data (buffer,time,node,data) VALUES ('%s',%s, %s,'%s')" % (self.bufferName, data[0], data[1][0], ','.join(str(x) for x in data[1][1:]) )  )
      dbCursor.close()
      self._conn.commit()
    except MySQLdb.Error as e:
      self.handleMySQLdbError (e)
    
  def retrieveItem(self):
    try:
      dbCursor = self._conn.cursor()
      dbCursor.execute ( "SELECT id, time, node, data FROM data WHERE processed = 0 and buffer = '%s' ORDER BY ID ASC LIMIT 1" % \
                          (self.bufferName,))
      id, time, node, data = dbCursor.fetchone()
      dbCursor.close()
      
      self._log.debug("MySQL (%s): retrieved stored input #%s from node %s (%s) at %s" % \
                     ( self.bufferName, id, node, data, time) )
    
      self.lastRetrievedItemId = id
      
      return [time,[node]+data.split(',')]
      
    except MySQLdb.Error as e:
      self.handleMySQLdbError (e)
      
    return [-1,""]
    
  def removeLastRetrievedItem(self):
    self._log.debug("MySQL (%s): marking %s as processed" % (self.bufferName, self.lastRetrievedItemId,))
    try:
      dbCursor = self._conn.cursor()
      dbCursor.execute ( "UPDATE data SET processed = unix_timestamp() WHERE id = %s" % (self.lastRetrievedItemId,) )
      dbCursor.close()
      self._conn.commit()
    except MySQLdb.Error as e:
      self.handleMySQLdbError (e)
    
  def size(self):
    size = 0
    try:
      dbCursor = self._conn.cursor()
      dbCursor.execute ( "SELECT count(*) from data where processed = 0 and buffer = '%s'" % \
                         (self.bufferName,))
      size = dbCursor.fetchone()[0]
      dbCursor.close()
    except MySQLdb.Error as e:
      self.handleMySQLdbError (e)
    if size != 0:
      self._log.debug("MySQL (%s): %d unprocessed items" % (self.bufferName, size) )
    return size